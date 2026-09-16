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
import kotlinx.coroutines.launch
import kotlinx.coroutines.runInterruptible
import kotlinx.coroutines.withContext
import kotlinx.serialization.KSerializer
import org.postgresql.PGConnection
import org.postgresql.core.Utils
import java.sql.Connection


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

        connectionScope.launch {
            try {
                awaitCancellation()
            } finally {
                withContext(Dispatchers.IO + NonCancellable) {
                    connection.close()
                }
            }
        }

        val pgConnection = withContext(Dispatchers.IO) {
            connection.prepareStatement("LISTEN $quotedName").use { it.execute() }
            connection.unwrap(PGConnection::class.java)
        }
        val flow = channelFlow {
            runInterruptible(Dispatchers.IO) {
                while (!Thread.currentThread().isInterrupted) {
                    val notifications = pgConnection.getNotifications(500)?.toList() ?: emptyList()
                    notifications
                        .forEach { notification ->
                            val element = mqJson.decodeFromString(serializer, notification.parameter)
                            launch { channel.send(element) }
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
