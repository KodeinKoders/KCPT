@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.kcpt.cron

import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.channels.consume
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.produceIn
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import net.kodein.kcpt.PGChannel
import net.kodein.kcpt.Source
import net.kodein.kcpt.getInstantOrNull
import net.kodein.kcpt.kcptJson
import net.kodein.kcpt.notify
import net.kodein.kcpt.openListenConnection
import net.kodein.kcpt.setInstant
import net.kodein.kcpt.transaction
import java.sql.Connection
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Instant
import kotlin.use


public interface Cron {
    public val name: String
    public val config: Config

    // Only one instant is ever in question: the boundary `to` shares with the next window's `from`.
    // `from` is always inclusive on both sides. Since Instant is a continuum with no "instant just
    // before to", this changes nothing about to's own computed value (Config.windowEnd's own comment
    // has the full reasoning) — only whether reaching it exactly counts as this window being done.
    public enum class WindowEnd { EXCLUSIVE, INCLUSIVE }

    public data class Config(
        val interval: Duration,
        val maxInterval: Duration = interval * 50,
        val retries: Int = 5,
        val tickTimeout: Duration = interval,
        val tickGrace: Duration = maxOf(tickTimeout * .2, MIN_TICK_GRACE),
        // EXCLUSIVE ([from, to)) is the more common convention for a contiguous range — length is
        // just to - from, no off-by-one — and is the default. INCLUSIVE ((from, to]) is offered for
        // callers whose own downstream data treats the boundary instant as belonging to this window
        // rather than the next one. Either way to's own value is unaffected: see WindowEnd's own
        // comment for why.
        val windowEnd: WindowEnd = WindowEnd.EXCLUSIVE,
    ) {
        init {
            // A tick is only ever materialized once its own window has fully elapsed, so at zero
            // every window would be zero-width and there would be nothing to ever wait for.
            require(interval > Duration.ZERO) { "interval must be positive, but was $interval" }
            // A catch-up chunk is clamped to processedUntil + maxInterval. Below interval, every
            // *normal* (non-catch-up) chunk would cover less ground than the cadence it advances
            // by, so materialization falls permanently further behind the clock.
            require(maxInterval >= interval) {
                "maxInterval must be at least interval ($interval), but was $maxInterval"
            }
            // Counts retries after the first failure, so 0 is legal (dead-letter on first
            // failure); negative would dead-letter a tick that has not failed yet.
            require(retries >= 0) { "retries must be at least 0, but was $retries" }
            // At zero withTimeout fires before the handler is entered and every lease reads as
            // already expired, so ticks are stolen from the servers still running them.
            require(tickTimeout > Duration.ZERO) { "tickTimeout must be positive, but was $tickTimeout" }
            // Too small — zero included — lets a reclaim happen before the holder's own timeout
            // fires, which inverts the guarantee the margin exists for.
            require(tickGrace >= MIN_TICK_GRACE) { "tickGrace must be at least $MIN_TICK_GRACE, but was $tickGrace" }
        }

        public companion object {
            // The lease holder's own withTimeout must always fire before another server may
            // reclaim, or a reclaim races the holder's own failure write-back and the same
            // window is ticked twice.
            public val MIN_TICK_GRACE: Duration = 200.milliseconds
        }
    }

    // No receive()/process() split, unlike MessageQueue: run() is both "find the next thing to
    // do" and "do it", because unlike a queue, a cron's own progress (which windows have been
    // materialized) is decoupled from any individual tick's outcome — there is nothing to hand
    // back to a caller in between. Multiple concurrent ticks come from calling run() more than
    // once (one call per server; also possible more than once per process), not from a knob here.
    public suspend fun run(
        // Independent of tickTimeout, which only bounds how long a lease may be held before
        // another server may reclaim it — so this must be able to exceed tickTimeout. A tick
        // allowed to run longer than that has its lease kept alive automatically, for exactly as
        // long as it runs — exactly like Message<T>.process()'s own automatic keepalive; there is
        // no parameter here to configure or misconfigure.
        timeout: Duration = config.tickTimeout,
        onSuccess: (from: Instant, to: Instant, notes: JsonElement) -> Unit = { _, _, _ -> },
        onFailure: (failure: CronTickFailure) -> JsonElement = { JsonObject(it.defaultNotes()) },
        onDeadLetter: (from: Instant, to: Instant, notes: JsonElement) -> Unit = { _, _, _ -> },
        // The materialize/lease/write-back queries themselves can throw (dropped connection,
        // exhausted pool) — unlike a tick failure, this has nothing to do with the user's
        // handler, so it is reported here rather than through onFailure, and run() backs off and
        // keeps going rather than dying, since run() returns Nothing and one blip must not kill
        // the cron forever.
        onError: (cause: Throwable) -> Unit = {},
        tick: suspend (from: Instant, to: Instant) -> JsonElement?,
    ): Nothing
}

public sealed class CronTickFailure {
    public abstract fun defaultNotes(): Map<String, JsonElement>

    public data class Exception(public val exception: Throwable) : CronTickFailure() {
        override fun defaultNotes(): Map<String, JsonElement> = mapOf(
            "cause" to JsonPrimitive("exception"),
            "exceptionMessage" to JsonPrimitive(exception.message),
            "exceptionType" to JsonPrimitive(exception::class.qualifiedName),
            "exceptionStackTrace" to JsonPrimitive(exception.stackTraceToString()),
        )
    }

    public data class Timeout(public val timeout: Duration) : CronTickFailure() {
        override fun defaultNotes(): Map<String, JsonElement> = mapOf(
            "cause" to JsonPrimitive("timeout"),
            "timeoutMs" to JsonPrimitive(timeout.inWholeMilliseconds),
        )
    }

    public data class Cancellation(public val cancelledFromWork: Boolean) : CronTickFailure() {
        override fun defaultNotes(): Map<String, JsonElement> = mapOf(
            "cause" to JsonPrimitive("cancellation"),
            "from" to JsonPrimitive(if (cancelledFromWork) "work" else "parent"),
        )
    }
}

private val leaseExpiredNote: JsonElement = JsonObject(mapOf("cause" to JsonPrimitive("leaseExpired")))

// Top-level rather than a CronImpl member, and extracted at all, so it's directly unit-testable at
// the exact boundary instant without constructing a CronImpl — a single-instant difference between
// the two conventions isn't something a real wall-clock integration test could ever pin down.
internal fun windowHasElapsed(now: Instant, to: Instant, windowEnd: Cron.WindowEnd): Boolean =
    when (windowEnd) {
        Cron.WindowEnd.EXCLUSIVE -> now >= to
        Cron.WindowEnd.INCLUSIVE -> now > to
    }

internal class CronImpl(
    override val name: String,
    override val config: Cron.Config,
    private val source: Source,
    private val tables: CronTables,
    // Only ever used by register(): the seed value a brand-new cursor row is created with.
    // Re-read every time register() runs (including the missing-cursor fallback in run()) rather
    // than captured as a single computed instant, so it stays a pure function of what the caller
    // originally asked for.
    private val startAt: Instant,
    private val runImmediately: Boolean,
) : Cron {

    init {
        require(name.length in 1..MAX_NAME_LENGTH) {
            "cron name must be 1..$MAX_NAME_LENGTH characters, but '$name' is ${name.length}"
        }
    }

    // Notified whenever a tick is materialized or a failed tick is released for immediate retry
    // — anything that makes a row newly available to lease. A reclaim (an expired lease) is
    // deliberately not notified: nothing changed that a *waiting* server couldn't already work
    // out for itself from started_at, exactly as messages never notify on their own reclaim path.
    private val channel = PGChannel(
        name = "${tables.cronTicks}_$name",
        serializer = Instant.serializer(),
    )

    internal suspend fun register(): Unit = withContext(Dispatchers.IO) {
        val nextSlotAt = if (runImmediately) startAt - config.interval else startAt
        source().use { connection ->
            connection.prepareStatement(
                "INSERT INTO ${tables.crons} (name, next_slot_at) VALUES (?, ?) ON CONFLICT (name) DO NOTHING"
            ).use { statement ->
                statement.setString(1, name)
                statement.setInstant(2, nextSlotAt)
                statement.executeUpdate()
            }
        }
    }

    // Advances the cursor by one window and materializes the tick row for it, if one's window
    // has fully elapsed. Returns whether it did, so the caller can loop immediately to catch up
    // further before ever attempting to lease anything — that's what lets several servers work
    // different catch-up chunks at once: each chunk commits (and notifies) independently, so an
    // idle server can start on chunk 1 the instant it lands, while this one moves on to chunk 2.
    private suspend fun tryMaterialize(): Boolean = withContext(Dispatchers.IO) {
        source().use { connection -> connection.attemptMaterialize() }
    }

    private suspend fun Connection.attemptMaterialize(): Boolean = transaction {
        val now = Clock.System.now()
        val nextSlotAt = prepareStatement(
            "SELECT next_slot_at FROM ${tables.crons} WHERE name = ? FOR UPDATE SKIP LOCKED"
        ).use { statement ->
            statement.setString(1, name)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) null else resultSet.getInstantOrNull(1)
            }
        } ?: return@transaction false

        // The cursor's own row is missing (dropped by hand) is handled by wakeUpAt() returning
        // null, not here — SKIP LOCKED cannot tell "no row" apart from "someone else has it".
        val nextGridBoundary = nextSlotAt + config.interval
        if (!windowHasElapsed(now, nextGridBoundary, config.windowEnd)) return@transaction false

        val behindBy = now - nextSlotAt
        // Steady state: exactly one grid slot, its end never looked at relative to "now" beyond
        // the threshold check above — this is what keeps ticks landing on the clock rather than
        // drifting by however long materialization happened to take. Only once genuinely behind
        // by more than one whole extra interval does this fall back to a maxInterval-capped,
        // now-clamped catch-up chunk. Unaffected by windowEnd: this is a separate "how far past
        // elapsed are we" policy, not a question of which side owns the boundary.
        val to = if (behindBy < config.interval * 2) nextGridBoundary
        else minOf(nextSlotAt + config.maxInterval, now)

        prepareStatement(
            "INSERT INTO ${tables.cronTicks} (name, from_at, to_at) VALUES (?, ?, ?) ON CONFLICT (name, from_at) DO NOTHING"
        ).use { statement ->
            statement.setString(1, name)
            statement.setInstant(2, nextSlotAt)
            statement.setInstant(3, to)
            statement.executeUpdate()
        }
        prepareStatement("UPDATE ${tables.crons} SET next_slot_at = ? WHERE name = ?").use { statement ->
            statement.setInstant(1, to)
            statement.setString(2, name)
            statement.executeUpdate()
        }
        notify(channel, to)
        true
    }

    private class RawTickRow(
        val from: Instant,
        val to: Instant,
        val startedAt: Instant?,
        val notesJson: String,
    )

    private sealed interface LeaseOutcome {
        object NoWork : LeaseOutcome
        class DeadLettered(val from: Instant, val to: Instant, val notes: JsonArray) : LeaseOutcome
        // startedAt is the exact value this lease was just stamped with — the same "now" the
        // UPDATE below wrote to started_at — so runTick can compute this tick's own nominal
        // deadline (startedAt + tickTimeout) without a second clock read that could disagree with
        // what the database actually holds.
        class Leased(val from: Instant, val to: Instant, val notes: JsonArray, val startedAt: Instant) : LeaseOutcome
    }

    private suspend fun tryLease(): LeaseOutcome = withContext(Dispatchers.IO) {
        source().use { connection -> connection.attemptLease() }
    }

    // A single pass: leases the next available tick, dead-letters a reclaim that has already run
    // out of retries, or reports there was nothing to take. Written as one straight-line sequence
    // of early returns — each `return@transaction` resolves the transaction {} lambda's own
    // result, not a non-local return out of this function.
    private fun Connection.attemptLease(): LeaseOutcome = transaction {
        val now = Clock.System.now()
        // A lease is only reclaimable once the holder's own tickTimeout has had the grace
        // margin to fire and record the failure itself.
        val reclaimableBefore = now - (config.tickTimeout + config.tickGrace)

        val row = prepareStatement(
            """
                SELECT from_at, to_at, started_at, notes
                FROM ${tables.cronTicks}
                WHERE name = ?
                  AND (started_at IS NULL OR started_at < ?)
                ORDER BY from_at
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """.trimIndent()
        ).use { statement ->
            statement.setString(1, name)
            statement.setInstant(2, reclaimableBefore)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) null
                else RawTickRow(
                    from = resultSet.getInstantOrNull(1)!!,
                    to = resultSet.getInstantOrNull(2)!!,
                    startedAt = resultSet.getInstantOrNull(3),
                    notesJson = resultSet.getString(4),
                )
            }
        } ?: return@transaction LeaseOutcome.NoWork

        val previousNotes = kcptJson.decodeFromString(JsonArray.serializer(), row.notesJson)
        // An expired lease: the previous holder died without reporting, so this counts as a
        // failed try, exactly as an explicit failure report would.
        val notes = if (row.startedAt != null) JsonArray(previousNotes + leaseExpiredNote) else previousNotes

        if (notes.size > config.retries) {
            // The crashed attempt just reclaimed was the last one allowed: dead-letter here,
            // rather than leasing a tick that would only be dead-lettered again once run.
            prepareStatement("DELETE FROM ${tables.cronTicks} WHERE name = ? AND from_at = ?").use { statement ->
                statement.setString(1, name)
                statement.setInstant(2, row.from)
                statement.executeUpdate()
            }
            return@transaction LeaseOutcome.DeadLettered(row.from, row.to, notes)
        }

        prepareStatement(
            "UPDATE ${tables.cronTicks} SET started_at = ?, notes = ?::json WHERE name = ? AND from_at = ?"
        ).use { statement ->
            statement.setInstant(1, now)
            statement.setString(2, kcptJson.encodeToString(JsonArray.serializer(), notes))
            statement.setString(3, name)
            statement.setInstant(4, row.from)
            statement.executeUpdate()
        }
        LeaseOutcome.Leased(row.from, row.to, notes, now)
    }

    // Deletes on success — there is nothing more to track for a window once it's done, unlike a
    // message queue's row this never had a payload worth keeping.
    private suspend fun recordSuccess(from: Instant): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.prepareStatement("DELETE FROM ${tables.cronTicks} WHERE name = ? AND from_at = ?").use { statement ->
                statement.setString(1, name)
                statement.setInstant(2, from)
                statement.executeUpdate()
            }
        }
    }

    private suspend fun recordFailure(
        from: Instant,
        to: Instant,
        priorNotes: JsonArray,
        failureNote: JsonElement,
        onDeadLetter: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
    ): Unit = withContext(Dispatchers.IO) {
        val notes = JsonArray(priorNotes + failureNote)
        if (notes.size > config.retries) {
            source().use { connection ->
                connection.prepareStatement("DELETE FROM ${tables.cronTicks} WHERE name = ? AND from_at = ?").use { statement ->
                    statement.setString(1, name)
                    statement.setInstant(2, from)
                    statement.executeUpdate()
                }
            }
            try { onDeadLetter(from, to, notes) } catch (_: Throwable) {}
        } else {
            source().use { connection ->
                connection.transaction {
                    // Released for immediate retry, so a server parked waiting must be told —
                    // exactly like a fresh materialization, and for the same reason: nothing else
                    // would wake it before its own reclaim-timeout check, minutes away.
                    prepareStatement(
                        "UPDATE ${tables.cronTicks} SET started_at = NULL, notes = ?::json WHERE name = ? AND from_at = ?"
                    ).use { statement ->
                        statement.setString(1, kcptJson.encodeToString(JsonArray.serializer(), notes))
                        statement.setString(2, name)
                        statement.setInstant(3, from)
                        statement.executeUpdate()
                    }
                    notify(channel, to)
                }
            }
        }
    }

    // Takes the stamp to write rather than reading its own now(): the caller (the keepalive loop)
    // needs that exact same instant afterward, to recompute this tick's own local expiresAt —
    // reading it once and sharing it is what keeps the two from disagreeing by however long the
    // round trip below takes.
    private suspend fun extendLease(from: Instant, now: Instant): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.prepareStatement("UPDATE ${tables.cronTicks} SET started_at = ? WHERE name = ? AND from_at = ?").use { statement ->
                statement.setInstant(1, now)
                statement.setString(2, name)
                statement.setInstant(3, from)
                statement.executeUpdate()
            }
        }
    }

    // Null only when the cursor row itself is missing (LEAST ignores a NULL argument in
    // PostgreSQL, so the whole expression is null only if *both* subqueries are: no cursor row,
    // and no currently-leased tick for this name — the latter can't happen without the former,
    // since only materialization, which requires the cursor, ever creates a tick row).
    //
    // Unaware of windowEnd, deliberately: this is a sleep-until hint, not the gate itself (that's
    // windowHasElapsed, in attemptMaterialize). Under INCLUSIVE, waking exactly at this instant
    // just means attemptMaterialize finds the window not yet elapsed and this loops back through
    // here once more — a fraction of a millisecond later, once real time has moved past it, the
    // very next attempt succeeds. Not worth the complexity of computing a separate, windowEnd-aware
    // wake instant for that.
    private suspend fun wakeUpAt(): Instant? = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.prepareStatement(
                """
                    SELECT LEAST(
                        (SELECT next_slot_at FROM ${tables.crons} WHERE name = ?) + ?::interval,
                        (SELECT min(started_at) FROM ${tables.cronTicks} WHERE name = ? AND started_at IS NOT NULL) + ?::interval
                    )
                """.trimIndent()
            ).use { statement ->
                statement.setString(1, name)
                statement.setString(2, "${config.interval.inWholeMilliseconds} ms")
                statement.setString(3, name)
                statement.setString(4, "${(config.tickTimeout + config.tickGrace).inWholeMilliseconds} ms")
                statement.executeQuery().use { resultSet ->
                    resultSet.next()
                    resultSet.getInstantOrNull(1)
                }
            }
        }
    }

    override suspend fun run(
        timeout: Duration,
        onSuccess: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
        onFailure: (failure: CronTickFailure) -> JsonElement,
        onDeadLetter: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
        onError: (cause: Throwable) -> Unit,
        tick: suspend (from: Instant, to: Instant) -> JsonElement?,
    ): Nothing {
        register()

        coroutineScope {
            // Captured explicitly rather than referencing a bare `this` inside async {} below,
            // which would resolve to the async coroutine's own (transient) scope instead of this
            // one — the exact deadlock openListenConnection's connection-keeper job hit before it
            // was parented to the wrong scope.
            val outerScope = this
            val flowDef: Deferred<PGChannel.ConnectionFlow<Instant>> = outerScope.async {
                source.openListenConnection(outerScope, channel)
            }

            // One subscription for the whole call, exactly like MessageQueue's own Receiver (G2):
            // opened once here, shared by every wait below, torn down when run() itself ends.
            suspend fun awaitSomethingToDo(wakeAt: Instant) {
                val notifications = flowDef.await().produceIn(outerScope)
                notifications.consume {
                    val wait = wakeAt - Clock.System.now()
                    // Any notification — or the deadline, whichever first — just means "go
                    // recheck everything from the top"; unlike messages, there is no "how soon"
                    // to compare it against, since a newly materialized or retried tick is always
                    // immediately leasable.
                    if (wait > Duration.ZERO) withTimeoutOrNull(wait) { notifications.receive() }
                }
            }

            try {
                while (true) {
                    try {
                        if (tryMaterialize()) continue

                        when (val outcome = tryLease()) {
                            is LeaseOutcome.DeadLettered ->
                                try { onDeadLetter(outcome.from, outcome.to, outcome.notes) } catch (_: Throwable) {}
                            is LeaseOutcome.Leased ->
                                runTick(outcome, timeout, onSuccess, onFailure, onDeadLetter, onError, tick)
                            is LeaseOutcome.NoWork -> {
                                val wakeAt = wakeUpAt()
                                if (wakeAt == null) register() else awaitSomethingToDo(wakeAt)
                            }
                        }
                    } catch (t: CancellationException) {
                        throw t
                    } catch (t: Throwable) {
                        try { onError(t) } catch (_: Throwable) {}
                        // No jittered backoff needed for a lost SKIP LOCKED race — there is always
                        // something else productive to try next (the other job, or the wait). This
                        // is purely to avoid spinning on a genuinely broken connection/pool.
                        delay(config.tickGrace)
                    }
                }
            } finally {
                if (flowDef.isCompleted && flowDef.getCompletionExceptionOrNull() == null) {
                    flowDef.getCompleted().close()
                }
                flowDef.cancel()
            }
        }
    }

    private suspend fun runTick(
        leased: LeaseOutcome.Leased,
        timeout: Duration,
        onSuccess: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
        onFailure: (failure: CronTickFailure) -> JsonElement,
        onDeadLetter: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
        onError: (cause: Throwable) -> Unit,
        tick: suspend (from: Instant, to: Instant) -> JsonElement?,
    ) {
        val from = leased.from
        val to = leased.to
        val priorNotes = leased.notes

        // This tick's nominal deadline, as originally stamped. A local var rather than a class
        // field — unlike Message<T>, there is no persistent per-tick object here for a keepalive
        // to update — updated in place by the keepalive loop below after each extendLease().
        var expiresAt = leased.startedAt + config.tickTimeout

        // Read once, rather than separately at each point below that needs "now" — the two checks
        // that follow reason about the same instant.
        val now = Clock.System.now()

        // A tick must never be attempted once its lease has already nominally expired: by that
        // point the row may already be visible to another server's reclaim query, and running the
        // handler here would be racing that server rather than genuinely holding the window. This
        // is a sign of a handover delay at least as large as the lease itself, not something to
        // quietly proceed through. Thrown here rather than swallowed: run()'s own outer catch still
        // contains it — reported through onError, the loop backs off by tickGrace and continues —
        // exactly as any other infrastructure surprise in this file already is.
        check(expiresAt - now > Duration.ZERO) {
            "This tick's lease already expired before processing could start " +
                "(expiresAt=$expiresAt, now=$now) — it may already be held by another server."
        }

        // failureNote is computed from onFailure, itself caller-supplied code, before entering
        // NonCancellable, matching Message<T>.process()'s onRequeueFailure guard: the write-back
        // and the report are both contained, so neither can escape run()'s loop and take the
        // cron down over one tick's or one report's failure.
        suspend fun writeFailure(failure: CronTickFailure): Unit = withContext(NonCancellable) {
            try {
                recordFailure(from, to, priorNotes, onFailure(failure), onDeadLetter)
            } catch (cause: Throwable) {
                try { onError(cause) } catch (_: Throwable) {}
            }
        }

        coroutineScope {
            try {
                withTimeout(timeout) {
                    // Needed in either of two cases. First, when timeout asks for more time than
                    // tickTimeout naturally covers: at timeout <= tickTimeout, the tick's own
                    // withTimeout above would otherwise always fire — and hand the window back —
                    // before the lease could go stale on its own. Second, even at
                    // timeout <= tickTimeout, when the delay between leasing this tick and
                    // actually starting to run it has already eaten enough of tickGrace's own
                    // margin that finishing within timeout from *here* could still land past the
                    // reclaim threshold (expiresAt + tickGrace) — a handover slower than tickGrace
                    // was sized for, which the simple timeout/tickTimeout comparison alone can't
                    // see. See <<safeguard-keepalive>> in the algorithm doc for the full reasoning
                    // — this mirrors Message<T>.process()'s own automatic keepalive exactly.
                    val keepAliveJob =
                        if (timeout > config.tickTimeout || now + timeout > expiresAt + config.tickGrace) {
                            launch(Dispatchers.Unconfined) {
                                while (true) {
                                    val remainingLease = expiresAt - Clock.System.now()
                                    delay(remainingLease * KEEPALIVE_LEASE_FRACTION)
                                    val extendedAt = Clock.System.now()
                                    extendLease(from, extendedAt)
                                    expiresAt = extendedAt + config.tickTimeout
                                }
                            }
                        } else null
                    // recordSuccess runs before onSuccess and before cancelling the keepalive job,
                    // so a throwing onSuccess cannot retroactively turn this success into a
                    // failure — the same reasoning as messages' aThrowingObserverDoesNotTurnA
                    // SuccessIntoAFailure.
                    val resultNotes = tick(from, to)
                    recordSuccess(from)
                    keepAliveJob?.cancelAndJoin()
                    try { onSuccess(from, to, resultNotes ?: JsonNull) } catch (_: Throwable) {}
                }
            } catch (_: TimeoutCancellationException) {
                writeFailure(CronTickFailure.Timeout(timeout))
            } catch (_: CancellationException) {
                // Read before entering NonCancellable: NonCancellable is an always-active Job
                // that would shadow this scope's Job and make isActive always report true.
                val cancelledFromWork = isActive
                writeFailure(CronTickFailure.Cancellation(cancelledFromWork))
                ensureActive()
            } catch (t: Throwable) {
                writeFailure(CronTickFailure.Exception(t))
            }
        }
    }

    private companion object {
        // The width of the name column declared in CronSetup.kt's CREATE TABLE.
        const val MAX_NAME_LENGTH = 32

        // The fraction of the remaining lease runTick's automatic keepalive waits before its next
        // extendLease(). Chosen so the cycle self-stabilizes with a comfortable margin: since
        // extendLease() always resets the lease to a fresh tickTimeout, this converges to
        // tickTimeout * 0.65 after the first cycle, leaving 35% of tickTimeout as headroom before
        // the next scheduled extendLease() — well clear of a reclaim, which only fires after
        // tickTimeout + tickGrace has passed since the last stamp. The same constant, and the
        // same reasoning, as kcpt-message-queue's Message.process().
        const val KEEPALIVE_LEASE_FRACTION = 0.65
    }
}
