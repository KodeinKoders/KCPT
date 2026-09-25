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
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant
import kotlin.time.toKotlinInstant

class CronCatchUpTest : AbstractPgsqlTest() {

    override val poolSize = 20

    // The spec's own worked example: interval 10 minutes, maxInterval 1 day. A cron whose cursor
    // was left at "now - 70 hours" (parts of 3 days behind) must catch up in exactly 3 back-to-
    // back chunks, each capped at maxInterval, the last one trimmed to now — and then settle into
    // the normal cadence.
    @Test
    fun catchesUpInMaxIntervalSizedChunksThenResumesNormalCadence() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 10.minutes, maxInterval = 1.days, retries = 3)

            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            sink.initialize()

            val now = Clock.System.now()
            val nextSlotAt = now - 70.hours
            dataSource.rawInsertCronCursor(prefix, name, nextSlotAt)

            val cron = sink.cron(name, config, startAt = nextSlotAt) // registration is a no-op: the row already exists

            val windows = CopyOnWriteArrayList<Pair<Instant, Instant>>()
            val runner = launch {
                cron.run { from, to -> windows += from to to; null }
            }

            withTimeoutOrNull(10.seconds) { awaitUntil { windows.size >= 3 } }
                ?: throw AssertionError("expected 3 catch-up ticks, only saw ${windows.size}: $windows")

            // A 4th tick would mean it kept firing back-to-back rather than settling down.
            delay(300.milliseconds)
            val sizeAfterSettling = windows.size

            runner.cancelAndJoin()

            assertEquals(3, sizeAfterSettling, "expected exactly 3 catch-up ticks: $windows")
            val (first, second, third) = windows

            approxEquals(nextSlotAt, first.first, "first window's from")
            approxEquals(first.first + 1.days, first.second, "first window's to (capped at maxInterval)")
            approxEquals(first.second, second.first, "second window chains onto the first")
            approxEquals(second.first + 1.days, second.second, "second window's to (capped at maxInterval)")
            approxEquals(second.second, third.first, "third window chains onto the second")
            approxEquals(now, third.second, "the last catch-up window should end at ~now")

            // The cursor only advances *past* a window when it's materialized, not when it
            // succeeds — it already sits at the third window's end and stays there until a full
            // interval has passed since.
            val cursorAfter = dataSource.cronNextSlotAtOf(prefix, name)!!.toInstant().toKotlinInstant()
            approxEquals(third.second, cursorAfter, "the cursor should already be at the last chunk's end")
        }
    }

    // Confirmed with the user: catch-up is concurrent, not strictly one-chunk-at-a-time. Chunk
    // boundaries only depend on the cursor and maxInterval, not on whether the previous chunk
    // finished, so several idle servers can each be mid-tick on a *different* chunk at once.
    @Test
    fun multipleServersWorkDifferentCatchUpChunksConcurrently() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 10.minutes, maxInterval = 1.days, retries = 3)

            val bootstrapSink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            bootstrapSink.initialize()
            val now = Clock.System.now()
            val nextSlotAt = now - 70.hours
            dataSource.rawInsertCronCursor(prefix, name, nextSlotAt)

            val inFlight = java.util.concurrent.atomic.AtomicInteger(0)
            val maxInFlight = java.util.concurrent.atomic.AtomicInteger(0)
            val completed = CopyOnWriteArrayList<Pair<Instant, Instant>>()

            val runners = (1..3).map {
                val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
                val cron = sink.cron(name, config, startAt = nextSlotAt)
                launch {
                    cron.run { from, to ->
                        val current = inFlight.incrementAndGet()
                        maxInFlight.updateAndGet { maxOf(it, current) }
                        try {
                            delay(200.milliseconds) // long enough for another server to overlap
                        } finally {
                            inFlight.decrementAndGet()
                        }
                        completed += from to to
                        null
                    }
                }
            }

            withTimeoutOrNull(10.seconds) { awaitUntil { completed.size >= 3 } }
                ?: throw AssertionError("expected all 3 catch-up chunks to complete: $completed")
            runners.forEach { it.cancelAndJoin() }

            assertTrue(maxInFlight.get() > 1, "expected more than one catch-up chunk in flight at once, max was ${maxInFlight.get()}")
        }
    }

    // shortenWindow answers "how much to trim off an on-time window" — a catch-up chunk is a
    // different question ("how much backlog to absorb in one go") and must ignore it entirely, even
    // when supplied a hook that would otherwise shrink every window down to almost nothing.
    @Test
    fun shortenWindowIsNotConsultedForACatchUpChunk() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 10.minutes, maxInterval = 1.days, retries = 3)

            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            sink.initialize()

            val now = Clock.System.now()
            val nextSlotAt = now - 70.hours
            dataSource.rawInsertCronCursor(prefix, name, nextSlotAt)

            val cron = sink.cron(name, config, startAt = nextSlotAt)

            val windows = CopyOnWriteArrayList<Pair<Instant, Instant>>()
            val runner = launch {
                cron.run(shortenWindow = { from, _ -> from + 1.seconds }) { from, to ->
                    windows += from to to
                    null
                }
            }

            withTimeoutOrNull(10.seconds) { awaitUntil { windows.size >= 3 } }
                ?: throw AssertionError("expected 3 catch-up ticks, only saw ${windows.size}: $windows")
            runner.cancelAndJoin()

            val (first, second, third) = windows
            approxEquals(nextSlotAt, first.first, "first window's from")
            approxEquals(first.first + 1.days, first.second, "first window's to (capped at maxInterval, not shortened)")
            approxEquals(first.second, second.first, "second window chains onto the first")
            approxEquals(second.first + 1.days, second.second, "second window's to (capped at maxInterval, not shortened)")
            approxEquals(second.second, third.first, "third window chains onto the second")
            approxEquals(now, third.second, "the last catch-up window should still end at ~now, not from + 1s")
        }
    }

    // A DB round trip can lose sub-second precision, so exact Instant equality is too strict here.
    private fun approxEquals(expected: Instant, actual: Instant, message: String) {
        assertTrue(
            (actual - expected).let { it.inWholeSeconds in -2..2 },
            "$message: expected ~$expected, was $actual",
        )
    }
}
