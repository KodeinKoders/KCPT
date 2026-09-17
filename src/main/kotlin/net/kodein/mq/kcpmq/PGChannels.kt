@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.awaitCancellation
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.SharingStarted
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
) {
    private val quotedName: String = Utils.escapeIdentifier(null, name).toString()

    init {
        val byteSize = name.toByteArray(Charsets.UTF_8).size
        require(byteSize in 1..63) {
            "PostgreSQL channel name must be 1..63 bytes (UTF-8), but '$name' is $byteSize bytes"
        }
    }

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

    public suspend fun openListenConnection(scope: CoroutineScope, source: Source): ConnectionFlow<T> {
        val connection = source()

        val connectionJob = Job(scope.coroutineContext[Job])
        val connectionScope = CoroutineScope(scope.coroutineContext + connectionJob)

        val connectionMutex = Mutex()

        connectionScope.launch {
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

        withContext(Dispatchers.IO) {
            connection.prepareStatement("LISTEN $quotedName").use { it.execute() }
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
                                    val element = mqJson.decodeFromString(serializer, notification.parameter)
                                    channel.send(element)
                                }
                            }
                    }
                }
            }
        }
        return ConnectionFlow(
            job = connectionJob,
            flow = flow.shareIn(connectionScope, SharingStarted.WhileSubscribed()),
        )
    }

    public suspend fun notify(connection: Connection, value: T): Unit =
        withContext(Dispatchers.IO) {
            connection.prepareStatement("SELECT pg_notify(?, ?)").use { statement ->
                statement.setString(1, name)
                statement.setString(2, mqJson.encodeToString(serializer, value))
                statement.execute()
            }
        }
}

public suspend fun <T> Connection.notify(channel: PGChannel<T>, value: T): Unit =
    channel.notify(this, value)

public suspend fun <T> Source.openListenConnection(scope: CoroutineScope, channel: PGChannel<T>): PGChannel.ConnectionFlow<T> =
    channel.openListenConnection(scope, this)
