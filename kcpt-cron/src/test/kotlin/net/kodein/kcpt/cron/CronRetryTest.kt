package net.kodein.kcpt.cron

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
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
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

class CronRetryTest : AbstractPgsqlTest() {

    override val poolSize = 20

    @Test
    fun aFailingTickIsRetriedImmediatelyOnTheIdenticalWindowThenDeadLettered() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            // A small interval so the window after the dead-lettered one becomes due quickly in
            // real time (this test runs on Dispatchers.Default, real wall-clock delays).
            val config = Cron.Config(interval = 300.milliseconds, retries = 2)

            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            val attempts = AtomicInteger(0)
            val windows = CopyOnWriteArrayList<Pair<Instant, Instant>>()
            val deadLettered = CopyOnWriteArrayList<Triple<Instant, Instant, Int>>()
            val nextTickWindows = CopyOnWriteArrayList<Pair<Instant, Instant>>()

            val cron = sink.cron(name, config, runImmediately = true)
            val runner = launch {
                cron.run(
                    onDeadLetter = { from, to, notes ->
                        deadLettered += Triple(from, to, (notes as kotlinx.serialization.json.JsonArray).size)
                    },
                ) { from, to ->
                    if (attempts.incrementAndGet() <= 3) {
                        windows += from to to
                        throw RuntimeException("boom $attempts")
                    }
                    nextTickWindows += from to to
                    null
                }
            }

            withTimeoutOrNull(8.seconds) { awaitUntil { deadLettered.size == 1 } }
                ?: throw AssertionError("expected exactly one dead-letter; attempts=$attempts windows=$windows")

            // Checked right after the dead-letter, before the 4th (successful) tick can also
            // increment attempts.
            assertEquals(3, attempts.get(), "retries=2 should allow 3 total attempts before dead-lettering")
            assertEquals(1, windows.distinct().size, "every retry must re-address the identical (from, to)")
            assertEquals(1, deadLettered.size)
            assertEquals(3, deadLettered[0].third, "onDeadLetter must be given all 3 accumulated failure notes")

            withTimeoutOrNull(5.seconds) { awaitUntil { nextTickWindows.isNotEmpty() } }
                ?: throw AssertionError("the cron did not continue after the dead letter")

            runner.cancelAndJoin()

            // The cron continued onto the next window rather than halting.
            assertTrue(
                nextTickWindows[0].first == windows[0].second,
                "the 4th tick should address the window right after the dead-lettered one",
            )
        }
    }

    @Test
    fun retriesZeroDeadLettersOnTheFirstFailure() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 10.minutes, retries = 0)

            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            val attempts = AtomicInteger(0)
            val deadLettered = AtomicInteger(0)

            val cron = sink.cron(name, config, runImmediately = true)
            val runner = launch {
                cron.run(onDeadLetter = { _, _, _ -> deadLettered.incrementAndGet() }) { _, _ ->
                    attempts.incrementAndGet()
                    throw RuntimeException("always fails")
                }
            }

            withTimeoutOrNull(5.seconds) { awaitUntil { deadLettered.get() >= 1 } }
                ?: throw AssertionError("expected an immediate dead-letter with retries = 0")
            runner.cancelAndJoin()

            assertEquals(1, attempts.get(), "retries = 0 must dead-letter on the very first failure")
        }
    }
}
