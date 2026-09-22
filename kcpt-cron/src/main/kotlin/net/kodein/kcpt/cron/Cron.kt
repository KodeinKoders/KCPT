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

    public data class Config(
        val interval: Duration,
        val maxInterval: Duration = interval * 50,
        val retries: Int = 5,
        val tickTimeout: Duration = interval,
        val tickGrace: Duration = maxOf(tickTimeout * .2, MIN_TICK_GRACE),
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
        // another server may reclaim it. A tick allowed to run longer than that needs a
        // keepalive to keep renewing the lease — exactly like Message<T>.process()'s own
        // timeout/keepalive pair — so this must be able to exceed tickTimeout. run() refuses the
        // combination that would let a handler outlive its own lease.
        timeout: Duration = config.tickTimeout,
        keepalive: Duration? = null,
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
        if (now < nextSlotAt + config.interval) return@transaction false

        val behindBy = now - nextSlotAt
        // Steady state: exactly one grid slot, its end never looked at relative to "now" beyond
        // the threshold check above — this is what keeps ticks landing on the clock rather than
        // drifting by however long materialization happened to take. Only once genuinely behind
        // by more than one whole extra interval does this fall back to a maxInterval-capped,
        // now-clamped catch-up chunk.
        val to = if (behindBy < config.interval * 2) nextSlotAt + config.interval
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
        class Leased(val from: Instant, val to: Instant, val notes: JsonArray) : LeaseOutcome
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
        LeaseOutcome.Leased(row.from, row.to, notes)
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

    private suspend fun extendLease(from: Instant): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.prepareStatement("UPDATE ${tables.cronTicks} SET started_at = ? WHERE name = ? AND from_at = ?").use { statement ->
                statement.setInstant(1, Clock.System.now())
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
        keepalive: Duration?,
        onSuccess: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
        onFailure: (failure: CronTickFailure) -> JsonElement,
        onDeadLetter: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
        onError: (cause: Throwable) -> Unit,
        tick: suspend (from: Instant, to: Instant) -> JsonElement?,
    ): Nothing {
        require(keepalive == null || keepalive < timeout) {
            "keepalive must be less than timeout ($timeout), but was $keepalive"
        }
        // The lease only survives tickTimeout + tickGrace past its last stamp, so a keepalive
        // slower than that lets it go stale between refreshes and another server reclaims the
        // window from under a handler that is still running. Checked against tickTimeout alone,
        // leaving tickGrace as headroom for the refresh's own round trip — the same margin
        // workerPool leaves against processingTimeout.
        require(keepalive == null || keepalive < config.tickTimeout) {
            "keepalive must be less than tickTimeout (${config.tickTimeout}), but was $keepalive"
        }
        // Nothing re-stamps the lease without a keepalive, so a handler allowed to outlive the
        // lease is *guaranteed* to still be running when another server reclaims its window: the
        // same window then runs twice, concurrently, and nothing anywhere reports it. This is the
        // one failure in this file that is silent on both sides, which is why it is refused here
        // rather than left to the caller to notice.
        require(keepalive != null || timeout <= config.tickTimeout + config.tickGrace) {
            "timeout ($timeout) is longer than a lease survives without being renewed " +
                "(tickTimeout + tickGrace = ${config.tickTimeout + config.tickGrace}): pass a " +
                "keepalive to hold the lease, raise tickTimeout, or lower timeout"
        }
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
                                runTick(outcome, timeout, keepalive, onSuccess, onFailure, onDeadLetter, onError, tick)
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
        keepalive: Duration?,
        onSuccess: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
        onFailure: (failure: CronTickFailure) -> JsonElement,
        onDeadLetter: (from: Instant, to: Instant, notes: JsonElement) -> Unit,
        onError: (cause: Throwable) -> Unit,
        tick: suspend (from: Instant, to: Instant) -> JsonElement?,
    ) {
        val from = leased.from
        val to = leased.to
        val priorNotes = leased.notes

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
                    val keepAliveJob = keepalive?.let {
                        launch(Dispatchers.Unconfined) {
                            while (true) {
                                delay(it)
                                extendLease(from)
                            }
                        }
                    }
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
    }
}
