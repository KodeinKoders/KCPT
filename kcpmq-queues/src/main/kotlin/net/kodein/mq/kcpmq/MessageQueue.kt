@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.CoroutineScope
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
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant
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
        // Floored at MIN_PROCESSING_GRACE, so a short processingTimeout cannot derive a margin too
        // small to cover the handover described there.
        val processingGrace: Duration = maxOf(processingTimeout * .2, MIN_PROCESSING_GRACE),
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
            // Too small — zero included — lets a reclaim happen before the holder's own timeout
            // fires, which inverts the guarantee the margin exists for.
            require(processingGrace >= MIN_PROCESSING_GRACE) {
                "processingGrace must be at least $MIN_PROCESSING_GRACE, but was $processingGrace"
            }
        }

        public companion object {
            // A lease starts when receive() takes it, not when the worker starts working: the
            // message still has to travel back out of receive() — which waits for its listening
            // connection to be torn down — and the worker still has to be dispatched. The margin
            // has to cover that handover, or the message is reclaimed and processed a second time
            // while its first worker is still going on to succeed. Nothing fails when that happens,
            // so it is invisible: the work is simply done twice.
            //
            // Teardown is bounded by PGChannel's notification poll (50ms); 200ms leaves room for
            // that plus a busy dispatcher, while staying small next to any realistic
            // processingTimeout.
            public val MIN_PROCESSING_GRACE: Duration = 200.milliseconds
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
    private val onUndecodableNotification:
        (channel: String, payload: String, cause: Throwable) -> Unit = { _, _, _ -> },
) : MessageQueue<T> {

    // The queue column is VARCHAR(32), so a longer name is accepted here and only fails at the
    // first post(), as a driver-level "value too long for type character varying(32)" that never
    // mentions which queue. Checked up front instead, before PGChannel's own 63-byte limit below
    // can reject a long name with a message about a channel the caller never named.
    init {
        require(name.length in 1..MAX_NAME_LENGTH) {
            "queue name must be 1..$MAX_NAME_LENGTH characters, but '$name' is ${name.length}"
        }
    }

    private val channel = PGChannel(
        name = "${tables.messages}_$name",
        serializer = Instant.serializer(),
        onUndecodableNotification = onUndecodableNotification,
    )

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
            connection.receiveNextOrNull(Clock.System.now())
        }
    }

    private suspend fun Connection.receiveNextOrNull(now: Instant): Message<T>? {
        transaction {
            while (true) {
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
                        ORDER BY due_at, id
                        LIMIT 1
                        FOR UPDATE SKIP LOCKED
                    """.trimIndent()
                ).use { statement ->
                    statement.setString(1, name)
                    statement.setInstant(2, now)
                    statement.setInstant(3, reclaimableBefore)
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
                    messageObservers.notifyEach { it.onFailure(candidate.id, candidate.payload, leaseExpiredNote) }
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
    private suspend fun Connection.recordUndecodable(raw: RawCandidate, now: Instant) {
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
            messageObservers.notifyEach { it.onUndecodable(raw.id, raw.payloadJson, undecodableNote) }
            return
        }

        val dueAt = now + config.processingTimeout
        prepareStatement("UPDATE ${tables.messages} SET due_at = ?, notes = ?::json, started_at = NULL WHERE id = ?").use { statement ->
            statement.setInstant(1, dueAt)
            statement.setString(2, mqJson.encodeToString(JsonArray.serializer(), notes))
            statement.setObject(3, raw.id.toJavaUuid())
            statement.executeUpdate()
        }
        // Announced before the commit that delivers it, like every other write that makes a
        // message available again. This consumer loops on immediately and does not need telling;
        // another one parked on a later deadline does.
        notify(channel, dueAt)
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
            channel = channel,
            observers = messageObservers,
        )

    override suspend fun receive(): Message<T> =
        coroutineScope {
            receiveNext()
        }

    // One pass of receiveNext's database work either takes a message or works out when the next one can become
    // available. Both outcomes leave the Dispatchers.IO block as a value because withContext's lambda is not
    // inline: the early return this code used to do from the middle of the block cannot cross it.
    private sealed interface ReceiveAttempt<T> {
        class Took<T>(val message: Message<T>) : ReceiveAttempt<T>
        class WaitUntil<T>(val instant: Instant) : ReceiveAttempt<T>
    }

    private suspend fun CoroutineScope.receiveNext(): Message<T> {
        source.openListenConnection(this, channel).use { flow ->
            while (true) {
                // Every statement below blocks the thread it runs on, and receive() is the hot path of every
                // consumer: workerPool calls it from Dispatchers.Default, whose threads the application needs for
                // its own coroutines. Only the database work moves — the wait loop below stays outside, since
                // parking there would hold an IO thread for as long as each idle consumer waits.
                val attempt = withContext(Dispatchers.IO) {
                    val now = Clock.System.now()
                    source().use { connection ->
                        val message = connection.receiveNextOrNull(now)
                        if (message != null) return@use ReceiveAttempt.Took(message)
                        val lease = config.processingTimeout + config.processingGrace
                        val leaseExpiresWhenStartedAt = now - lease
                        // Here we take the next due message in the strict future (not including now).
                        // If there are messages due now or in the past, we know that they are being processed since
                        // receiveNextOrNull returned null (so they were skipped).
                        // Note that if a new message was posted due now or in the past in the interval between the
                        // receiveNextOrNull call and this query, it will not be caught by this query, but the loop will
                        // immediately restart because of the notification in the channel.
                        val next = connection.prepareStatement(
                            """
                                SELECT LEAST(
                                    (
                                        SELECT min(due_at)
                                        FROM ${tables.messages}
                                        WHERE
                                                queue = ?
                                            AND started_at IS NULL
                                            AND due_at > ?
                                    ),
                                    (
                                        SELECT min(started_at)
                                        FROM ${tables.messages}
                                        WHERE
                                                queue = ?
                                            AND started_at IS NOT NULL
                                            AND started_at > ?
                                    ) + ?::interval
                                )
                                """.trimIndent()
                        ).use { statement ->
                            statement.setString(1, name)
                            statement.setInstant(2, now)
                            statement.setString(3, name)
                            statement.setInstant(4, leaseExpiresWhenStartedAt)
                            statement.setString(5, "${lease.inWholeMilliseconds} ms")
                            statement.executeQuery().use { resultSet ->
                                resultSet.next()
                                resultSet.getInstantOrNull(1)
                            }
                        } ?: run {
                            // Note that if a message was posted to the queue exactly between this tryReceive and this flow
                            // collection, the notification will still be received, as the flow was created before the
                            // [tryReceive]. (Creating the flow with [openListenConnection] opens the connection to
                            // Postgres, and therefore all notifications sent between the flow creation and its collection
                            // are buffered).
                            Clock.System.now() + config.securityCheckTimeout
                        }
                        ReceiveAttempt.WaitUntil(next)
                    }
                }
                var next = when (attempt) {
                    is ReceiveAttempt.Took -> return attempt.message
                    is ReceiveAttempt.WaitUntil -> attempt.instant
                }
                while (Clock.System.now() < next) {
                    val notification = withTimeoutOrNull(next - Clock.System.now()) { flow.first() }
                    if (notification != null && notification < next) next = notification
                }
            }
        }
    }

    private companion object {
        // The width of the queue column declared in Setup.kt's CREATE TABLE.
        const val MAX_NAME_LENGTH = 32
    }
}
