@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
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


public class PGChannel<T>(
    public val name: String,
    private val serializer: KSerializer<T>,
    private val source: Source,
) {
    private val quotedName: String = Utils.escapeIdentifier(StringBuilder(), name).toString()

    init {
        val byteSize = name.toByteArray(Charsets.UTF_8).size
        require(byteSize in 1..63) {
            "PostgreSQL channel name must be 1..63 bytes (UTF-8), but '$name' is $byteSize bytes"
        }
    }

    public suspend fun openListenConnection(scope: CoroutineScope): Flow<T> {
        val connection = source()
        scope.coroutineContext[Job]!!.invokeOnCompletion { connection.close() }

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
        return flow
            .shareIn(scope, SharingStarted.WhileSubscribed())
    }

    public suspend fun notify(value: T): Unit =
        withContext(Dispatchers.IO) {
            source().use { connection ->
                connection.prepareStatement("SELECT pg_notify(?, ?)").use { statement ->
                    statement.setString(1, name)
                    statement.setString(2, mqJson.encodeToString(serializer, value))
                    statement.execute()
                }
            }
        }
}
