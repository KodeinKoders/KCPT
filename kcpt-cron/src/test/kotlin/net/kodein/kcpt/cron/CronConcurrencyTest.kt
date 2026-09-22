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
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

// The whole point of the two-table redesign: ticks are independent rows, so several servers can
// each be mid-tick on a *different* window at once — while the one thing that must never happen
// (the same window leased by two servers at once) still never does.
class CronConcurrencyTest : AbstractPgsqlTest() {

    override val poolSize = 20

    @Test
    fun noTwoServersEverLeaseTheIdenticalWindowAtOnce() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            // Each tick outlasts the interval, so several windows are expected to be in flight at
            // once across the 5 servers — that's not what's under test here.
            val config = Cron.Config(interval = 100.milliseconds, tickTimeout = 5.seconds, retries = 5)
            val activeHoldersPerWindow = ConcurrentHashMap<Pair<Instant, Instant>, AtomicInteger>()
            val maxConcurrentHoldersForAnyWindow = AtomicInteger(0)
            val completed = CopyOnWriteArrayList<Pair<Instant, Instant>>()

            val runners = (1..5).map {
                val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
                val cron = sink.cron(name, config)
                launch {
                    cron.run { from, to ->
                        val window = from to to
                        val holders = activeHoldersPerWindow.computeIfAbsent(window) { AtomicInteger(0) }
                        val current = holders.incrementAndGet()
                        maxConcurrentHoldersForAnyWindow.updateAndGet { maxOf(it, current) }
                        // finally, not just a trailing decrement: cancelAndJoin below tears the 5
                        // runners down one at a time, so a still-in-flight delay() on one of them
                        // gets cancelled while the others are still running — without this, that
                        // cancelled holder never decrements, and the row it releases (correctly,
                        // via the production retry path) can be picked up by a still-running
                        // server, reading as a false "2 concurrent holders" that never really
                        // overlapped.
                        try {
                            delay(150.milliseconds) // outlasts the interval
                        } finally {
                            holders.decrementAndGet()
                        }
                        completed += window
                        null
                    }
                }
            }

            awaitUntil(timeout = 10.seconds) { completed.size >= 8 }
            runners.forEach { it.cancelAndJoin() }

            assertEquals(
                1, maxConcurrentHoldersForAnyWindow.get(),
                "the same window must never be leased by two servers at once",
            )
        }
    }

    // The actual behaviour the user asked for: with interval = 300ms and one server's tick taking
    // 2s (much longer than interval), the other idle servers must keep ticking every interval on
    // *later* windows rather than waiting for the slow one to finish.
    @Test
    fun idleServersWorkLaterWindowsWhileAnotherIsStillOnAnEarlierOne() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 300.milliseconds, tickTimeout = 5.seconds, retries = 5)

            // Whichever server's lease wins the very first window plays the deliberately-slow
            // role — it's not predetermined which one that will be.
            val firstWindowClaimed = AtomicBoolean(false)
            val slowTickInProgress = AtomicBoolean(false)
            val overlapObserved = AtomicBoolean(false)
            val fastCompletions = CopyOnWriteArrayList<Pair<Instant, Instant>>()

            val runners = (1..5).map {
                val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
                val cron = sink.cron(name, config)
                launch {
                    cron.run { from, to ->
                        if (firstWindowClaimed.compareAndSet(false, true)) {
                            slowTickInProgress.set(true)
                            try {
                                delay(2.seconds) // much longer than interval, comfortably under tickTimeout
                            } finally {
                                slowTickInProgress.set(false)
                            }
                        } else {
                            if (slowTickInProgress.get()) overlapObserved.set(true)
                            fastCompletions += from to to
                        }
                        null
                    }
                }
            }

            awaitUntil(timeout = 10.seconds) { fastCompletions.size >= 2 }
            runners.forEach { it.cancelAndJoin() }

            assertTrue(
                overlapObserved.get(),
                "expected at least one later window to be processed by an idle server while the " +
                    "first window's slow tick was still in flight",
            )
        }
    }
}
