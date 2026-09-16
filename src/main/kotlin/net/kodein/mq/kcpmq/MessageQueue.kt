@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.KSerializer
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import java.sql.Connection
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
        val processingTimeout: Duration = 1.minutes,
        // Margin added to processingTimeout before another consumer may reclaim a lease, so that a
        // worker's own processing timeout always fires first and requeues the message itself.
        // Without it the two deadlines coincide and a reclaim can race an in-flight requeue.
        val processingGrace: Duration = processingTimeout * .2,
    )
}

private val leaseExpiredNote: JsonElement = JsonObject(mapOf("cause" to JsonPrimitive("leaseExpired")))

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

    private class Candidate<T>(
        val id: Uuid,
        val payload: T,
        val notes: JsonArray,
        val startedAt: Instant?,
    )

    override suspend fun tryReceive(): Message<T>? = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.transaction { receiveNext() }
        }
    }

    private fun Connection.receiveNext(): Message<T>? {
        while (true) {
            val now = Clock.System.now()
            // A lease is only reclaimable once the holder's own processing timeout has had the
            // grace margin to fire and requeue the message itself.
            val reclaimableBefore = now - (config.processingTimeout + config.processingGrace)

            val candidate = prepareStatement(
                """
                    SELECT id, payload, notes, started_at
                    FROM ${tables.messages}
                    WHERE
                            queue = ?
                        AND due_at <= ?
                        AND (started_at IS NULL OR started_at < ?)
                        AND json_array_length(notes) < ?
                    ORDER BY due_at, id
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                """.trimIndent()
            ).use { statement ->
                statement.setString(1, name)
                statement.setInstant(2, now)
                statement.setInstant(3, reclaimableBefore)
                statement.setInt(4, config.maxTries)
                statement.executeQuery().use { resultSet ->
                    if (!resultSet.next()) null
                    else Candidate(
                        id = (resultSet.getObject(1) as UUID).toKotlinUuid(),
                        payload = Json.decodeFromString(serializer, resultSet.getString(2)),
                        notes = Json.decodeFromString(JsonArray.serializer(), resultSet.getString(3)),
                        startedAt = resultSet.getInstantOrNull(4),
                    )
                }
            } ?: return null

            if (candidate.startedAt == null) {
                prepareStatement("UPDATE ${tables.messages} SET started_at = ? WHERE id = ?").use { statement ->
                    statement.setInstant(1, now)
                    statement.setObject(2, candidate.id.toJavaUuid())
                    statement.executeUpdate()
                }
                return message(candidate.id, candidate.payload, candidate.notes, now)
            }

            // An expired lease: the previous holder died without reporting, so this counts as a
            // failed try, exactly as an explicit failureRequeue would.
            val notes = JsonArray(candidate.notes + leaseExpiredNote)
            if (notes.size >= config.maxTries) {
                prepareStatement("DELETE FROM ${tables.messages} WHERE id = ?").use { statement ->
                    statement.setObject(1, candidate.id.toJavaUuid())
                    statement.executeUpdate()
                }
                commit()
                messageObservers.forEach { it.onFailure(candidate.id, candidate.payload, leaseExpiredNote) }
                continue
            }
            prepareStatement("UPDATE ${tables.messages} SET started_at = ?, notes = ?::json WHERE id = ?").use { statement ->
                statement.setInstant(1, now)
                statement.setString(2, mqJson.encodeToString(JsonArray.serializer(), notes))
                statement.setObject(3, candidate.id.toJavaUuid())
                statement.executeUpdate()
            }
            return message(candidate.id, candidate.payload, notes, now)
        }
    }

    private fun message(id: Uuid, payload: T, notes: List<JsonElement>, now: Instant): Message<T> =
        MessageImpl(
            id = id,
            payload = payload,
            receivedAt = now,
            mqConfig = config,
            notes = notes,
            source = source,
            tables = tables,
            observers = messageObservers,
        )

    override suspend fun receive(): Message<T> =
        coroutineScope {
            source.openListenConnection(this, channel).use { flow ->
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
}
