@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.KSerializer
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.serializer
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
    ) {
        // Every one of these fails silently rather than loudly when it is out of range, so they are
        // rejected at construction. init also covers copy(), so a config derived from a valid one
        // cannot slip through either.
        init {
            // Bound straight into `json_array_length(notes) < ?`, so below 1 nothing is ever
            // selectable — not even a message that has never been tried. It would then never be
            // leased, never accrue a note and so never dead-letter: posts would simply accumulate,
            // invisible, while consumers sat idle.
            require(maxTries >= 1) { "maxTries must be at least 1, but was $maxTries" }
            // receive() waits until now + securityCheckTimeout, so at zero the wait loop is skipped
            // entirely and the retry loop spins on the database as fast as it can.
            require(securityCheckTimeout > Duration.ZERO) {
                "securityCheckTimeout must be positive, but was $securityCheckTimeout"
            }
            // At zero a message expires the instant it is received: process() starts already past
            // its deadline, and every lease reads as expired, so messages are reclaimed from under
            // the workers still holding them.
            require(processingTimeout > Duration.ZERO) {
                "processingTimeout must be positive, but was $processingTimeout"
            }
            // Negative would let a reclaim happen before the holder's own timeout fires, which
            // inverts the guarantee the margin exists for.
            require(processingGrace >= Duration.ZERO) {
                "processingGrace cannot be negative, but was $processingGrace"
            }
        }
    }
}

private val leaseExpiredNote: JsonElement = JsonObject(mapOf("cause" to JsonPrimitive("leaseExpired")))

private val undecodableNote: JsonElement = JsonObject(mapOf("cause" to JsonPrimitive("undecodable")))

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

    private class RawCandidate(
        val id: Uuid,
        val payloadJson: String,
        val notesJson: String,
        val startedAt: Instant?,
    )

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

            val raw = prepareStatement(
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
                    else RawCandidate(
                        id = (resultSet.getObject(1) as UUID).toKotlinUuid(),
                        payloadJson = resultSet.getString(2),
                        notesJson = resultSet.getString(3),
                        startedAt = resultSet.getInstantOrNull(4),
                    )
                }
            } ?: return null

            // Decoding happens after the row is selected but before the lease is taken, so a
            // payload this consumer cannot read is recorded as a failed try and skipped over
            // rather than thrown out of the transaction — otherwise it would roll back untouched,
            // stay first in line, and wedge the queue behind it forever.
            val candidate = decode(raw) ?: run {
                recordUndecodable(raw, now)
                continue
            }

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

    private fun decode(raw: RawCandidate): Candidate<T>? =
        try {
            Candidate(
                id = raw.id,
                payload = mqJson.decodeFromString(serializer, raw.payloadJson),
                notes = mqJson.decodeFromString(JsonArray.serializer(), raw.notesJson),
                startedAt = raw.startedAt,
            )
        } catch (_: SerializationException) {
            null
        }

    // An undecodable payload is counted as a failed try like any other failure, rather than
    // dropped outright: the usual cause is a consumer that is behind the producer, and rolling it
    // forward makes the message readable again. due_at is pushed out so the retries are spread
    // over time instead of being burnt through in a tight loop.
    private fun Connection.recordUndecodable(raw: RawCandidate, now: Instant) {
        val previousNotes =
            try { mqJson.decodeFromString(JsonArray.serializer(), raw.notesJson) }
            catch (_: SerializationException) { JsonArray(emptyList()) }
        val notes = JsonArray(previousNotes + undecodableNote)

        if (notes.size >= config.maxTries) {
            prepareStatement("DELETE FROM ${tables.messages} WHERE id = ?").use { statement ->
                statement.setObject(1, raw.id.toJavaUuid())
                statement.executeUpdate()
            }
            commit()
            messageObservers.forEach { it.onUndecodable(raw.id, raw.payloadJson, undecodableNote) }
            return
        }

        prepareStatement("UPDATE ${tables.messages} SET due_at = ?, notes = ?::json, started_at = NULL WHERE id = ?").use { statement ->
            statement.setInstant(1, now + config.processingTimeout)
            statement.setString(2, mqJson.encodeToString(JsonArray.serializer(), notes))
            statement.setObject(3, raw.id.toJavaUuid())
            statement.executeUpdate()
        }
        commit()
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
