package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
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
    @Test
    fun aSlowTickDoesNotShiftTheGridForLaterTicks() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val interval = 300.milliseconds
            val config = Cron.Config(interval = interval, tickTimeout = 5.seconds)

            // Not runImmediately: that seeds the cursor a full interval early, halving the margin
            // before the catch-up branch kicks in — on a fresh prefix, real schema-init latency
            // can exceed that margin with an interval this short, misclassifying tick 0 as a
            // catch-up tick. Letting the first tick wait one full interval like normal avoids the
            // confound; it's not what this test is about.
            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            val cron = sink.cron(name, config)
            val windows = CopyOnWriteArrayList<Pair<Instant, Instant>>()
            val tickIndex = AtomicInteger(0)

            val runner = launch {
                cron.run { from, to ->
                    // Tick 1 alone overruns the interval by 150ms; every other tick is fast.
                    if (tickIndex.getAndIncrement() == 1) delay(450.milliseconds)
                    windows += from to to
                    null
                }
            }

            withTimeoutOrNull(10.seconds) { awaitUntil { windows.size >= 4 } }
                ?: throw AssertionError("expected at least 4 ticks, only saw ${windows.size}")
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
