@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import java.util.*
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.minutes
import kotlin.time.ExperimentalTime
import kotlin.time.Instant
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid
import kotlin.uuid.toJavaUuid
import kotlin.uuid.toKotlinUuid


public interface MessageQueue<T> {
    public val name: String
    public val config: Config

    public suspend fun post(value: T, due: Instant? = null)
    public suspend fun tryReceive(): Message<T>?
    public suspend fun receive(): Message<T>

    public data class Config(
        val maxTries: Int = 5,
        val securityCheckTimeout: Duration = 10.minutes,
        val messageTreatmentTimeout: Duration = 1.minutes,
    )
}

internal class MessageQueueImpl<T> internal constructor(
    override val name: String,
    private val source: Source,
    private val tables: MQTables,
    private val serializer: KSerializer<T>,
    override val config: MessageQueue.Config,
    private val messageObservers: List<Message.Observer<T>>,
) : MessageQueue<T> {

    @OptIn(ExperimentalTime::class)
    private val channel = PGChannel(
        name = "${tables.messages}_$name",
        serializer = Instant.serializer(),
    )

    @OptIn(ExperimentalUuidApi::class)
    override suspend fun post(
        value: T,
        due: Instant?,
    ) = withContext(Dispatchers.IO) {
        val serializedValue = mqJson.encodeToString(serializer, value)
        val id = Uuid.generateV7()
        val dueAt = due ?: Clock.System.now()
        withContext(Dispatchers.IO) {
            source().use { connection ->
                connection.transaction {
                    prepareStatement("""
                        INSERT INTO ${tables.messages}
                        (
                            queue,
                            id,
                            due_at,
                            payload
                        )
                        VALUES (?, ?, ?, ?::json)
                    """.trimIndent()).use { statement ->
                            statement.setString(1, name)
                            statement.setObject(2, id.toJavaUuid())
                            statement.setInstant(3, dueAt)
                            statement.setString(4, serializedValue)
                            statement.executeUpdate()
                        }
                    notify(channel, dueAt)
                }
            }
        }
    }

    override suspend fun tryReceive(): Message<T>? = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.transaction {
                val message = connection.prepareStatement(
                    "SELECT id, payload, notes FROM ${tables.messages} WHERE queue = ? AND due_at <= ? AND started_at IS NULL AND json_array_length(notes) < ? LIMIT 1 FOR UPDATE SKIP LOCKED"
                ).use { statement ->
                    statement.setString(1, name)
                    statement.setInstant(2, Clock.System.now())
                    statement.setInt(3, config.maxTries)
                    statement.executeQuery().use { resultSet ->
                        if (!resultSet.next()) return@withContext null
                        val id = (resultSet.getObject(1) as UUID).toKotlinUuid()
                        val payloadJson = resultSet.getString(2)
                        val payload = Json.decodeFromString(serializer, payloadJson)
                        val notesJson = resultSet.getString(3)
                        val notes = Json.decodeFromString(JsonArray.serializer(), notesJson)
                        MessageImpl(
                            id = id,
                            payload = payload,
                            notes = notes,
                            allowRequeue = notes.size < config.maxTries,
                            source = source,
                            tables = tables,
                            observers = messageObservers,
                        )
                    }
                }
                connection.prepareStatement("UPDATE ${tables.messages} SET started_at = ? WHERE id = ?").use { statement ->
                    statement.setInstant(1, Clock.System.now())
                    statement.setObject(2, message.id.toJavaUuid())
                    statement.executeUpdate()
                }
                message
            }
        }
    }

    override suspend fun receive(): Message<T> =
        coroutineScope {
            val flow = source.openListenConnection(this, channel)
            var message: Message<T>?
            do {
                message = tryReceive()
                if (message == null) {
                    var next = source().use { connection ->
                        connection.prepareStatement(
                            "SELECT min(due_at) FROM ${tables.messages} WHERE queue = ? AND started_at IS NULL AND json_array_length(notes) < ?"
                        ).use { statement ->
                            statement.setString(1, name)
                            statement.setInt(2, config.maxTries)
                            statement.executeQuery().use { resultSet ->
                                resultSet.next()
                                resultSet.getInstantOrNull(1)
                            }
                        }
                    } ?: run {
                        // This timeout is a security measure in case a message was posted to the queue without
                        // notifying the channel. This should never happen (unless some non-KCMP code writes into the
                        // message table).
                        // Note that if a message was posted to the queue exactly between this tryReceive and this flow
                        // collection, the notification will still be received, as the flow was created before the
                        // [tryReceive]. (Creating the flow with [openListenConnection] opens the connection to
                        // Postgres, and therefore all notifications sent between the flow creation and its collection
                        // are buffered).
                        Clock.System.now() + config.securityCheckTimeout
                    }
                    while (Clock.System.now() < next) {
                        val notification = withTimeoutOrNull(next - Clock.System.now()) { flow.first() }
                        if (notification != null && notification < next) next = notification
                    }
                }
            } while (message == null)
            message
        }
}
