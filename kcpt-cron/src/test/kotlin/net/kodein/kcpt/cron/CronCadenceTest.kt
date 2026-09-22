package net.kodein.kcpt.cron

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kodein.kcpt.AbstractPgsqlTest
import net.kodein.kcpt.asSource
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

// The actual fix, isolated from concurrency: a tick's window must land on a fixed grid derived
// from when the cron started, not drift by however long a tick took to run.
class CronCadenceTest : AbstractPgsqlTest() {

    // With a single consumer, materializing the window right after a slow tick is the one case
    // that can actually observe a difference between "snap to the exact grid boundary" and
    // "clamp to whatever now happens to be": while tick 1 is busy overrunning the interval, this
    // sole consumer can't get back to materializing tick 2's window until tick 1 finishes — by
    // then, real time has already passed tick 2's boundary. Tick 2 must still land exactly where
    // it always would have; a design that clamps to "now" instead would show tick 2 arriving late
    // by however long tick 1 overran.
    //
    // This only stays correct — attemptMaterialize takes the steady-state (exact grid) branch
    // rather than the catch-up (clamped-to-now) one — as long as tick 1's overrun leaves the
    // cursor less than interval*2 behind by the time this sole consumer gets back around to
    // materializing tick 2. That margin (interval*2 - the overrun) has to absorb every bit of real
    // latency between tick 1 finishing and tick 2's SELECT running: two real Postgres round trips
    // (tick 1's own success DELETE, tick 2's materialize SELECT ... FOR UPDATE), a connection-pool
    // checkout, and ordinary coroutine/JVM scheduling — all real, not simulated, since this suite
    // runs on Dispatchers.Default against a genuine Testcontainers Postgres, not virtual time.
    // interval and the overrun below are sized so that margin is several seconds, comfortably
    // above the worst latency spike observed in practice (just over a second, seen once under
    // three modules' Testcontainers suites contending for the same CPU at once) — a short interval
    // with a small overrun leaves only tens of milliseconds of margin, which such a spike blows
    // through outright, misclassifying tick 2 as a catch-up tick and failing this test for a
    // reason that has nothing to do with the grid-snap logic actually being wrong.
    @Test
    fun aSlowTickDoesNotShiftTheGridForLaterTicks() = runTest(timeout = 25.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val interval = 3.seconds
            // Comfortably above the 3.4s tick-1 overrun below, so the tick's own timeout is never
            // what's in question here.
            val config = Cron.Config(interval = interval, tickTimeout = 10.seconds)

            // Not runImmediately: that seeds the cursor a full interval early, halving the margin
            // before the catch-up branch kicks in. Letting the first tick wait one full interval
            // like normal avoids that confound; it's not what this test is about.
            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            val cron = sink.cron(name, config)
            val windows = CopyOnWriteArrayList<Pair<Instant, Instant>>()
            val tickIndex = AtomicInteger(0)

            val runner = launch {
                cron.run { from, to ->
                    // Tick 1 alone overruns the interval by 400ms; every other tick is fast. That
                    // leaves a margin of interval*2 - 3.4s = 2.6s before the catch-up branch would
                    // wrongly kick in for tick 2 — see the class-level comment for why that much
                    // margin, rather than a bare handful of milliseconds, is the actual fix here.
                    if (tickIndex.getAndIncrement() == 1) delay(3400.milliseconds)
                    windows += from to to
                    null
                }
            }

            // Only tick 2 (materialized right after tick 1's overrun) is the case this test is
            // actually about; a third window is one more than the minimum, for a little extra
            // confidence that the cron keeps ticking normally afterward. awaitUntil's own default
            // timeout is 5 seconds — far too short once the healthy path alone takes ~9.5s at this
            // interval, so it must be passed explicitly here rather than relied on via a wrapping
            // withTimeoutOrNull, which would never get the chance to matter.
            awaitUntil(timeout = 20.seconds) { windows.size >= 3 }
            runner.cancelAndJoin()

            val first = windows[0].first
            for ((index, window) in windows.withIndex()) {
                val expectedFrom = first + interval * index
                val expectedTo = expectedFrom + interval
                assertTrue(
                    (window.first - expectedFrom).let { it.inWholeMilliseconds in -50..50 },
                    "window $index's from should be on the grid at $expectedFrom, was ${window.first}",
                )
                assertTrue(
                    (window.second - expectedTo).let { it.inWholeMilliseconds in -50..50 },
                    "window $index's to should be on the grid at $expectedTo, was ${window.second}",
                )
            }
        }
    }
}
