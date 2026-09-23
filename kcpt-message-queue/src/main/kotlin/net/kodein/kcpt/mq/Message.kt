@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.kcpt.mq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import net.kodein.kcpt.PGChannel
import net.kodein.kcpt.Source
import net.kodein.kcpt.kcptJson
import net.kodein.kcpt.notify
import net.kodein.kcpt.setInstant
import net.kodein.kcpt.transaction
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Instant
import kotlin.uuid.Uuid
import kotlin.uuid.toJavaUuid


public interface Message<T> {
    public val id: Uuid
    public val payload: T
    public val receivedAt: Instant
    public val expiresAt: Instant
    // The queue's own reclaim margin, exposed so process() can tell whether the delay between
    // receiving this message and actually starting to work it has already eaten into it — see
    // process()'s own keepalive condition.
    public val processingGrace: Duration
    public val notes: List<JsonElement>
    public suspend fun successDelete(notes: JsonElement = JsonNull)
    public suspend fun failureDelete(notes: JsonElement = JsonNull)
    public suspend fun failureRequeue(due: Instant? = null, notes: JsonElement = JsonNull)
    public suspend fun extend()

    public interface Observer<in T> {
        public fun onSuccess(id: Uuid, payload: T, notes: JsonElement)
        public fun onFailure(id: Uuid, payload: T, notes: JsonElement)

        // Dropped after its payload could not be decoded, so there is no T to report — the raw
        // JSON is given instead, since it is the only remaining record of the message.
        public fun onUndecodable(id: Uuid, payloadJson: String, notes: JsonElement) {}
    }
}

// Observers are caller-supplied code sitting on the acknowledgement path, after the row has already
// been written. One that throws must not hide the observers behind it, and must not reach the
// caller: successDelete is called from inside process()'s try, where an observer's exception is
// indistinguishable from a processing failure, and process() would go on to requeue a message it
// has just deleted — reporting the same message as both a success and a failure.
//
// Throwable, with no re-raise of CancellationException: these callbacks are not suspend, so they
// cannot legitimately take part in cancellation, and letting one through would reopen this very
// hole by another route.
internal inline fun <T> List<Message.Observer<T>>.notifyEach(block: (Message.Observer<T>) -> Unit) {
    forEach {
        try {
            block(it)
        } catch (_: Throwable) {
        }
    }
}

internal class MessageImpl<T>(
    override val id: Uuid,
    override val payload: T,
    override val receivedAt: Instant,
    private val mqConfig: MessageQueue.Config,
    override val notes: List<JsonElement>,
    private val source: Source,
    private val tables: MessageQueueTables,
    // Carried only so every statement below can filter on it alongside id: the primary key is
    // (queue, id), and a statement matching id alone cannot use that index as a direct lookup.
    private val queue: String,
    // Requeuing makes this message available again, which a consumer already parked in receive()
    // has no other way of learning — so it is announced on the same channel post() uses.
    private val channel: PGChannel<Instant>,
    private val observers: List<Message.Observer<T>> = emptyList(),
) : Message<T> {

    // extend() writes this from Dispatchers.IO while the worker reads it from wherever it is
    // running — process()'s default timeout is derived from it, and it is public API besides. When
    // process()'s automatic keepalive is active, the two run concurrently for the whole of the
    // work, and nothing else establishes a happens-before edge between them, so without @Volatile a
    // reader can go on seeing an indefinitely stale deadline. The value is only ever written whole,
    // never compound-updated, so volatile is enough.
    @Volatile
    override var expiresAt: Instant = receivedAt + mqConfig.processingTimeout
        private set

    // A plain read-through: mqConfig never changes after construction, so this needs neither its
    // own field nor @Volatile.
    override val processingGrace: Duration get() = mqConfig.processingGrace

    // Source only moves connection *acquisition* to Dispatchers.IO, so without this the blocking
    // statement would run on whichever dispatcher the caller is on: a core-count-limited pool it
    // can starve, a single thread it can freeze, or — via process()'s Unconfined keepalive — the
    // shared timer thread, where it would hold up every delay() in the process.
    override suspend fun successDelete(notes: JsonElement): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.prepareStatement("DELETE FROM ${tables.messages} WHERE queue = ? AND id = ?").use { statement ->
                statement.setString(1, queue)
                statement.setObject(2, id.toJavaUuid())
                statement.executeUpdate()
            }
        }
        observers.notifyEach { it.onSuccess(id, payload, notes) }
    }

    override suspend fun failureRequeue(due: Instant?, notes: JsonElement): Unit = withContext(Dispatchers.IO) {
        val updatedNotes = JsonArray(this@MessageImpl.notes + notes)
        if (updatedNotes.size >= mqConfig.maxTries) {
            failureDelete(notes)
            return@withContext
        }

        val dueAt = due ?: Clock.System.now()
        source().use { connection ->
            // The update and the notification share a transaction, exactly as in post(). Postgres
            // only delivers NOTIFY on commit, and that ordering is the point: a notification that
            // arrived before its row was visible would wake a consumer, show it nothing, and send
            // it back to sleep on a later deadline — worse than never notifying at all.
            connection.transaction {
                prepareStatement("UPDATE ${tables.messages} SET due_at = ?, notes = ?::json, started_at = NULL WHERE queue = ? AND id = ?").use { statement ->
                    statement.setInstant(1, dueAt)
                    statement.setString(2, kcptJson.encodeToString(JsonArray.serializer(), updatedNotes))
                    statement.setString(3, queue)
                    statement.setObject(4, id.toJavaUuid())
                    statement.executeUpdate()
                }
                notify(channel, dueAt)
            }
        }
    }

    override suspend fun failureDelete(notes: JsonElement): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.prepareStatement("DELETE FROM ${tables.messages} WHERE queue = ? AND id = ?").use { statement ->
                statement.setString(1, queue)
                statement.setObject(2, id.toJavaUuid())
                statement.executeUpdate()
            }
        }
        observers.notifyEach { it.onFailure(id, payload, notes) }
    }

    override suspend fun extend(): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            val now = Clock.System.now()
            connection.prepareStatement("UPDATE ${tables.messages} SET started_at = ? WHERE queue = ? AND id = ?").use { statement ->
                statement.setInstant(1, now)
                statement.setString(2, queue)
                statement.setObject(3, id.toJavaUuid())
                statement.executeUpdate()
            }
            expiresAt = now + mqConfig.processingTimeout
        }
    }
}

public sealed class MessageProcessingFailure {
    public abstract fun defaultNotes(): Map<String, JsonElement>

    public data class Exception(public val exception: Throwable) : MessageProcessingFailure() {
        override fun defaultNotes(): Map<String, JsonElement> =
            mapOf(
                "cause" to JsonPrimitive("exception"),
                "exceptionMessage" to JsonPrimitive(exception.message),
                "exceptionType" to JsonPrimitive(exception::class.qualifiedName),
                "exceptionStackTrace" to JsonPrimitive(exception.stackTraceToString()),
            )
    }
    public data class Timeout(public val timeout: Duration) : MessageProcessingFailure() {
        override fun defaultNotes(): Map<String, JsonElement> =
            mapOf(
                "cause" to JsonPrimitive("timeout"),
                "timeoutMs" to JsonPrimitive(timeout.inWholeMilliseconds),
            )
    }
    public data class Cancellation(public val cancelledFromWork: Boolean) : MessageProcessingFailure() {
        override fun defaultNotes(): Map<String, JsonElement> =
            mapOf(
                "cause" to JsonPrimitive("cancellation"),
                "from" to JsonPrimitive(if (cancelledFromWork) "work" else "parent"),
            )
    }
}

// The fraction of the remaining lease process()'s automatic keepalive waits before its next
// extend(). Chosen so the cycle self-stabilizes with a comfortable margin: since extend() always
// resets the lease to a fresh processingTimeout, this converges to processingTimeout * 0.65 after
// the first cycle, leaving 35% of processingTimeout as headroom before the next scheduled extend()
// — well clear of a reclaim, which only fires after processingTimeout + processingGrace has passed
// since the last stamp.
private const val KEEPALIVE_LEASE_FRACTION = 0.65

public suspend fun <T> Message<T>.process(
    timeout: Duration = this.expiresAt - Clock.System.now(),
    onFailure: (failure: MessageProcessingFailure) -> JsonElement = { JsonObject(it.defaultNotes()) },
    onRequeueFailure: (message: Message<T>, failure: MessageProcessingFailure, cause: Throwable) -> Unit = { _, _, _ -> },
    process: suspend (Message<T>) -> JsonElement?
) {
    // The nominal lease this message was handed: expiresAt was stamped at receivedAt + the queue's
    // processingTimeout. Read as a difference rather than against Clock.System.now(), because the
    // default timeout above was itself computed from a now() a moment earlier — comparing against a
    // fresh now() would make the default just barely fail its own check. Used below purely to decide
    // whether a keepalive is needed at all — not to validate a caller-supplied one, since there is
    // no longer a keepalive parameter to get wrong.
    val lease = expiresAt - receivedAt

    // Read once, rather than separately at each point below that needs "now" — the two checks that
    // follow reason about the same instant.
    val now = Clock.System.now()

    // A handler must never be attempted once its lease has already nominally expired: by that
    // point the row may already be visible to another consumer's reclaim query, and running the
    // handler here would be racing that consumer rather than genuinely holding the message. This
    // is a sign of a delay far larger than processingGrace was ever sized to absorb — a saturated
    // dispatcher, a paused process — not something to quietly proceed through and hope for the
    // best.
    check(expiresAt - now > Duration.ZERO) {
        "This message's lease already expired before processing could start " +
            "(expiresAt=$expiresAt, now=$now) — it may already be held by another consumer."
    }

    // failureRequeue talks to the database, so a dropped connection or an exhausted pool makes it
    // throw. Left unguarded, that exception would leave process() from inside a catch block,
    // escape workerPool's launch, and cancel the pool's scope — one message's transient blip
    // killing every other message in flight.
    //
    // Nothing is lost by containing it here: the UPDATE that would have cleared started_at is the
    // one that failed, so the lease stands and the row stays invisible to other consumers until it
    // expires; receiveNextOrNull then reclaims it, appends leaseExpiredNote and counts the try,
    // exactly as this requeue would have. The only cost is that redelivery waits for
    // processingTimeout + processingGrace instead of happening now.
    //
    // The caller hears about it through onRequeueFailure and nowhere else: there is no logging in
    // this library, and failureRequeue deliberately notifies no observer. onRequeueFailure is
    // itself caller-supplied code on a failure path, so it is guarded in turn — a throw from it
    // would reopen this very hole.
    //
    // Catches Throwable flat, CancellationException included: this runs under NonCancellable, so
    // the caller's own cancellation cannot arrive here as an exception — it is re-raised from the
    // scope's Job by the ensureActive() below, outside this guard. Anything caught here is the
    // requeue's own failure.
    suspend fun requeue(failure: MessageProcessingFailure): Unit = withContext(NonCancellable) {
        try {
            failureRequeue(notes = onFailure(failure))
        } catch (cause: Throwable) {
            try { onRequeueFailure(this@process, failure, cause) } catch (_: Throwable) {}
        }
    }

    coroutineScope {
        try {
            withTimeout(timeout) {
                // Needed in either of two cases. First, when timeout asks for more time than the
                // lease naturally covers: at timeout <= lease, the handler's own withTimeout above
                // would otherwise always fire — and hand the message back — before the lease could
                // go stale on its own. Second, even at timeout <= lease, when the delay between
                // receiving this message and actually starting to work it (dispatch queueing, GC,
                // anything else) has already eaten enough of processingGrace's own margin that
                // finishing within timeout from *here* could still land past the reclaim threshold
                // (expiresAt + processingGrace) — a handover slower than processingGrace was sized
                // for, which the simple timeout/lease comparison alone can't see.
                val keepAliveJob =
                    if (timeout > lease || now + timeout > expiresAt + processingGrace) {
                        launch(Dispatchers.Unconfined) {
                            while (true) {
                                // Recomputed every cycle from the live remaining lease, not a fixed
                                // duration: extend() resets expiresAt to a fresh processingTimeout,
                                // so after the first cycle this converges to a stable
                                // processingTimeout * KEEPALIVE_LEASE_FRACTION wait, leaving
                                // (1 - KEEPALIVE_LEASE_FRACTION) of the lease as margin before the
                                // next scheduled extend() — comfortably ahead of a reclaim, which
                                // only fires after the full processingTimeout + processingGrace has
                                // passed since the last stamp.
                                val remainingLease = expiresAt - Clock.System.now()
                                delay(remainingLease * KEEPALIVE_LEASE_FRACTION)
                                extend()
                            }
                        }
                    } else null
                val notes = process(this@process)
                successDelete(notes ?: JsonNull)
                keepAliveJob?.cancelAndJoin()
            }
        } catch (_: TimeoutCancellationException) {
            requeue(MessageProcessingFailure.Timeout(timeout))
        } catch (_: CancellationException) {
            // Read before entering NonCancellable: NonCancellable is an always-active Job that
            // would shadow this scope's Job and make isActive always report true.
            val cancelledFromWork = isActive
            requeue(MessageProcessingFailure.Cancellation(cancelledFromWork))
            ensureActive()
        } catch (t: Throwable) {
            requeue(MessageProcessingFailure.Exception(t))
        }
    }
}
