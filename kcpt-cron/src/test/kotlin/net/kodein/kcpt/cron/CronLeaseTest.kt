package net.kodein.kcpt.cron

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import net.kodein.kcpt.AbstractPgsqlTest
import net.kodein.kcpt.asSource
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.time.toKotlinInstant

class CronLeaseTest : AbstractPgsqlTest() {

    override val poolSize = 20

    // Simulates a server that leased a tick and then crashed: started_at is set, but nothing ever
    // clears it. Another server (here, the only server) must eventually reclaim it rather than
    // waiting forever, and must resume the *same* window rather than a fresh one.
    @Test
    fun aStuckLeaseIsEventuallyReclaimedOnTheSamePinnedWindow() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 300.milliseconds, tickTimeout = 300.milliseconds, retries = 5)

            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            sink.initialize()

            val from = Clock.System.now() - 300.milliseconds
            val pinnedTo = Clock.System.now()
            // The cursor sits just past this window, and the tick row itself was leased a moment
            // ago — well past tickTimeout + tickGrace by the time run() looks.
            dataSource.rawInsertCronCursor(prefix, name, pinnedTo)
            dataSource.rawInsertCronTick(prefix, name, fromAt = from, toAt = pinnedTo, startedAt = Clock.System.now() - 5.seconds)

            val cron = sink.cron(name, config, startAt = pinnedTo) // registration is a no-op: the cursor row already exists
            val windows = CopyOnWriteArrayList<Pair<Instant, Instant>>()
            // The row is deleted by the success write-back right after the tick returns, so its
            // notes must be read from inside the tick itself, while the row still exists.
            val notesDuringTick = CopyOnWriteArrayList<String>()
            val runner = launch {
                cron.run { f, t ->
                    notesDuringTick += dataSource.cronTickNotesJson(prefix, name, f)
                    windows += f to t
                    null
                }
            }

            withTimeoutOrNull(8.seconds) { awaitUntil { windows.isNotEmpty() } }
                ?: throw AssertionError("the stuck lease was never reclaimed")
            runner.cancelAndJoin()

            assertEquals(1, windows.size)
            assertEquals(pinnedTo.epochSeconds, windows[0].second.epochSeconds, "the reclaim must resume the pinned window, not a fresh one")

            // The reclaim itself counted as a failed try.
            assertTrue(
                "leaseExpired" in notesDuringTick.firstOrNull().orEmpty(),
                "the reclaim must be recorded as a failed try: $notesDuringTick",
            )
        }
    }

    // A tick that legitimately runs longer than tickTimeout must not be stolen out from under its
    // own holder while its keepalive is still renewing the lease.
    @Test
    fun keepaliveProtectsALongTickFromBeingReclaimed() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 5.seconds, tickTimeout = 400.milliseconds, retries = 5)

            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            val attempts = AtomicInteger(0)
            val cron = sink.cron(name, config, runImmediately = true)

            val runner = launch {
                // timeout is independent of tickTimeout — tickTimeout only bounds the lease, and
                // a tick allowed to run longer than that needs its own, larger timeout here, with
                // keepalive renewing the lease underneath it.
                cron.run(timeout = 2.seconds, keepalive = 100.milliseconds) { from, to ->
                    attempts.incrementAndGet()
                    delay(1.seconds) // longer than tickTimeout, but kept alive
                    null
                }
            }

            delay(1500.milliseconds) // long past tickTimeout without the keepalive
            runner.cancelAndJoin()

            assertEquals(1, attempts.get(), "the tick must not have been reclaimed and re-run while its keepalive was active")
        }
    }

    // The two configurations that would let a handler outlive its own lease — and so have the
    // same window silently run twice, on two servers, with nothing reporting it — are refused up
    // front rather than left to be discovered in production.
    @Test
    fun aKeepaliveThatCannotOutpaceTheLeaseIsRejected() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val cron = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .cron(uniqueQueueName(), Cron.Config(interval = 10.minutes, tickTimeout = 1.minutes))

            // Refreshes every 2 minutes, but the lease goes stale 1 minute (+ grace) after each
            // stamp: between refreshes the window is reclaimable out from under the handler.
            val failure = assertFailsWith<IllegalArgumentException> {
                cron.run(timeout = 5.minutes, keepalive = 2.minutes) { _, _ -> null }
            }
            assertTrue(
                "tickTimeout" in (failure.message ?: ""),
                "the message must name tickTimeout, which is what the keepalive has to outpace: ${failure.message}",
            )
        }
    }

    @Test
    fun aTimeoutOutlastingTheLeaseWithNoKeepaliveIsRejected() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val cron = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .cron(uniqueQueueName(), Cron.Config(interval = 10.minutes, tickTimeout = 1.minutes))

            // Nothing re-stamps started_at, so at tickTimeout + tickGrace the window is reclaimed
            // while this handler still has four minutes of its timeout left to run.
            val failure = assertFailsWith<IllegalArgumentException> {
                cron.run(timeout = 5.minutes) { _, _ -> null }
            }
            assertTrue(
                "keepalive" in (failure.message ?: ""),
                "the message must point at the missing keepalive: ${failure.message}",
            )
        }
    }

    // The mirror of the two above: a long timeout *with* a keepalive fast enough to hold the
    // lease is the supported way to run a tick longer than tickTimeout, and must not be refused.
    @Test
    fun aLongTimeoutWithAnAdequateKeepaliveIsAccepted() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val cron = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .cron(uniqueQueueName(), Cron.Config(interval = 10.minutes, tickTimeout = 1.minutes))

            val runner = launch {
                cron.run(timeout = 5.minutes, keepalive = 30.seconds) { _, _ -> null }
            }
            delay(300.milliseconds)
            assertTrue(runner.isActive, "this combination is the supported one and must be accepted")
            runner.cancelAndJoin()
        }
    }
}
