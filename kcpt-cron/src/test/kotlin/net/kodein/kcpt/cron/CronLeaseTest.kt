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
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
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
    // own holder while its automatic keepalive is still renewing the lease. No keepalive argument
    // to pass anymore — timeout (2s) alone exceeds tickTimeout (400ms), which is enough for
    // runTick to decide on its own that one is needed.
    //
    // Needs a second server: a single cron.run() call processes ticks one at a time, awaiting
    // runTick before its own loop can look at anything else, so nothing in a one-server version of
    // this test could ever re-lease the tick regardless of whether the keepalive actually ran —
    // the assertion would hold either way. The second server is what makes "not reclaimed" an
    // actual claim rather than a foregone conclusion.
    @Test
    fun aTimeoutLongerThanTickTimeoutKeepsTheLeaseAliveAutomatically() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 5.seconds, tickTimeout = 400.milliseconds, retries = 5)
            val attempts = AtomicInteger(0)

            suspend fun server() {
                val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
                val cron = sink.cron(name, config, runImmediately = true)
                // timeout is independent of tickTimeout — tickTimeout only bounds the lease, and
                // a tick allowed to run longer than that gets its own larger timeout here, with
                // the lease kept alive automatically underneath it.
                cron.run(timeout = 2.seconds) { from, to ->
                    attempts.incrementAndGet()
                    delay(1.seconds) // longer than tickTimeout, but kept alive
                    null
                }
            }

            val runners = listOf(launch { server() }, launch { server() })

            delay(1500.milliseconds) // long past tickTimeout+tickGrace without a keepalive
            runners.forEach { it.cancelAndJoin() }

            assertEquals(1, attempts.get(), "the tick must not have been reclaimed and re-run by the other server while its keepalive was active")
        }
    }

    // A tick must never be attempted once its lease has already nominally expired by the time
    // processing starts. There is no public seam to inject an artificial delay between leasing and
    // running a tick the way there is for messages (tryReceive() then process() separately) — the
    // two happen back to back inside run()'s own private loop — so an artificially tiny
    // tickTimeout stands in for a genuinely pathological handover delay instead: an ordinary
    // Postgres round trip between leasing and starting to run reliably exceeds 1ms on its own.
    //
    // The check throws inside runTick, which is exactly what run()'s own outer catch already
    // exists to contain — reported through onError, the loop backs off by tickGrace and continues,
    // the same as any other infrastructure surprise in this file.
    @Test
    fun aTickThatCannotStartInTimeIsReportedThroughOnError() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val config = Cron.Config(interval = 100.milliseconds, tickTimeout = 1.milliseconds, retries = 5)

            val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
            val cron = sink.cron(name, config, runImmediately = true)
            val errors = CopyOnWriteArrayList<Throwable>()

            val runner = launch {
                cron.run(onError = { errors += it }) { _, _ -> null }
            }

            awaitUntil(timeout = 5.seconds) { errors.any { it is IllegalStateException } }
            runner.cancelAndJoin()

            assertTrue(
                errors.any { it is IllegalStateException && "already expired" in (it.message ?: "") },
                "expected an IllegalStateException about an already-expired lease, got: $errors",
            )
        }
    }
}
