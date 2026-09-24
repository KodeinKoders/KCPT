@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.kcpt

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
import kotlinx.coroutines.flow.buffer
import kotlinx.coroutines.flow.channelFlow
import kotlinx.coroutines.flow.shareIn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import org.postgresql.PGConnection
import org.postgresql.core.Utils
import java.sql.Connection
import kotlin.coroutines.cancellation.CancellationException


// getNotifications blocks for up to this long, and the listening loop only notices it has been
// cancelled between polls — so this is also how long closing a listening connection takes. That
// cost lands on receive(), which waits for the teardown before handing back a message whose lease
// is already running, so it is spent out of the message's processing budget. It must therefore stay
// well under the smallest permitted processingGrace; at 500ms it consumed the whole margin of a
// short-timeout queue and messages were reclaimed and processed twice.
private const val NOTIFICATION_POLL_MS: Int = 50

public class PGChannel<T>(
    public val name: String,
    private val serializer: KSerializer<T>,
    // Told about a notification arriving on this channel whose payload cannot be decoded. Such a
    // notification is dropped, so without this hook nothing in the process would report that a
    // producer is speaking a format this listener cannot read.
    private val onUndecodableNotification:
        (channel: String, payload: String, cause: Throwable) -> Unit = { _, _, _ -> },
) {
    private val quotedName: String = Utils.escapeIdentifier(null, name).toString()

    init {
        val byteSize = name.toByteArray(Charsets.UTF_8).size
        require(byteSize in 1..63) {
            "PostgreSQL channel name must be 1..63 bytes (UTF-8), but '$name' is $byteSize bytes"
        }
    }

    /**
     * The notifications arriving on a listening connection.
     *
     * Collect it once, for as long as the notifications are wanted. Sharing is eager and keeps no
     * replay, so a notification emitted while nothing is collecting is dropped rather than held:
     * a collector that resubscribes per event — `while (true) { flow.first() }` — is deaf between
     * events, and whatever arrives in that window is lost. Hold one collection across the work
     * instead, with [produceIn][kotlinx.coroutines.flow.produceIn] or your own buffering collector.
     */
    public class ConnectionFlow<T> internal constructor(
        private val job: Job,
        private val flow: Flow<T>
    ) : Flow<T> by flow, AutoCloseable {
        override fun close() {
            job.cancel()
        }
        public suspend fun closeAndJoin() {
            job.cancelAndJoin()
        }
    }

    // The handler is caller-supplied code on the listening path, so it is not allowed to fail the
    // flow either — that would re-create, by a second route, exactly the breakage it reports.
    private fun report(payload: String, cause: Throwable) {
        try {
            onUndecodableNotification(name, payload, cause)
        } catch (_: Throwable) {
        }
    }

    public suspend fun openListenConnection(scope: CoroutineScope, source: Source): ConnectionFlow<T> {
        // Everything below wires one connection to one scope, and half-done is worse than not at
        // all. A scope that is already cancelled is the ordinary case at shutdown — workerPool
        // cancels while receive() is looping — so refuse before taking a connection from the pool.
        scope.ensureActive()

        val connection = source()

        // LISTEN before the teardown is armed, so nothing can close this connection while the
        // statement is in flight. The other order let a cancelling receive() fail with
        // PSQLException("This statement has been closed") instead of CancellationException, and
        // workerPool reads that as a genuine failure and takes every in-flight worker down with it.
        // Until the teardown exists this function is the connection's only owner, so whatever is
        // thrown here is its to clean up — under NonCancellable, since the throw is usually the
        // cancellation that would otherwise skip the close being unwound for.
        try {
            withContext(Dispatchers.IO) {
                connection.prepareStatement("LISTEN $quotedName").use { it.execute() }
            }
        } catch (cause: Throwable) {
            withContext(Dispatchers.IO + NonCancellable) { connection.close() }
            throw cause
        }

        val connectionJob = Job(scope.coroutineContext[Job])
        val connectionScope = CoroutineScope(scope.coroutineContext + connectionJob)

        val connectionMutex = Mutex()

        // The ATOMIC start below is what takes the DelicateCoroutinesApi opt-in: that coroutine exists
        // only to give the connection back, and a coroutine cancelled before it starts never runs its
        // body at all — so started normally it would skip its own finally and strand the connection
        // acquired just above for the life of the process. This is the case the coroutines
        // documentation names for ATOMIC: a resource taken outside the coroutine is still released when
        // cancellation lands between taking it and the launch.
        @OptIn(DelicateCoroutinesApi::class)
        connectionScope.launch(start = CoroutineStart.ATOMIC) {
            try {
                awaitCancellation()
            } finally {
                withContext(Dispatchers.IO + NonCancellable) {
                    connectionMutex.withLock {
                        try {
                            connection.prepareStatement("UNLISTEN $quotedName").use { it.execute() }
                        } finally {
                            connection.close()
                        }
                    }
                }
            }
        }

        val flow = channelFlow {
            withContext(Dispatchers.IO) {
                val pgConnection = connection.unwrap(PGConnection::class.java)
                connectionMutex.withLock {
                    while (isActive) {
                        val notifications = pgConnection.getNotifications(NOTIFICATION_POLL_MS)?.toList() ?: emptyList()
                        notifications
                            .forEach { notification ->
                                // Ignore stale notifications.
                                if (notification.name == name) {
                                    val element =
                                        try {
                                            kcptJson.decodeFromString(serializer, notification.parameter)
                                        } catch (cause: CancellationException) {
                                            throw cause
                                        } catch (cause: Throwable) {
                                            // Not narrowed to SerializationException: a built-in serializer like
                                            // Instant's can throw a bare IllegalArgumentException directly
                                            // (Instant.parse does, on a validly-JSON-encoded but unparseable
                                            // string) without ever wrapping it in one, and serializer is
                                            // caller-supplied besides — this loop must survive whatever it
                                            // throws, the same way report() already survives whatever
                                            // onUndecodableNotification throws. CancellationException is
                                            // re-thrown above rather than reported, since it is never a
                                            // decoding failure.
                                            report(notification.parameter, cause)
                                            return@forEach
                                        }
                                    channel.send(element)
                                }
                            }
                    }
                }
            }
        }
            .shareIn(connectionScope, SharingStarted.Eagerly)
            .buffer(Channel.UNLIMITED)
        return ConnectionFlow(
            job = connectionJob,
            flow = flow,
        )
    }

    public suspend fun notify(connection: Connection, value: T): Unit =
        withContext(Dispatchers.IO) {
            connection.prepareStatement("SELECT pg_notify(?, ?)").use { statement ->
                statement.setString(1, name)
                statement.setString(2, kcptJson.encodeToString(serializer, value))
                statement.execute()
            }
        }
}

public suspend fun <T> Connection.notify(channel: PGChannel<T>, value: T): Unit =
    channel.notify(this, value)

public suspend fun <T> Source.openListenConnection(scope: CoroutineScope, channel: PGChannel<T>): PGChannel.ConnectionFlow<T> =
    channel.openListenConnection(scope, this)
