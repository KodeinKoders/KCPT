package net.kodein.mq.kcpmq

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

// Mirrors MessageProcessTest's cancellation contract, adapted to run()'s update-in-place shape:
// the same three-way distinction (timeout, caller cancellation, foreign cancellation from the
// tick lambda) has to hold here too.
class CronRunCancellationTest : AbstractPgsqlTest() {

    override val poolSize = 20

    private fun sink(prefix: String) = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)

    @Test
    fun aTimedOutTickIsRecordedAsAFailureAndTheLeaseIsReleasedImmediately() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val theSink = sink(prefix)
            val cron = theSink.cron(name, Cron.Config(interval = 10.minutes, tickTimeout = 300.milliseconds), runImmediately = true)

            // A single active consumer immediately re-leases a just-released row itself, so
            // checking started_at == null after some delay would race against that same
            // consumer's own next attempt. Counting attempts instead is race-free: it can only
            // reach 2+ if the first timed-out lease was genuinely released for retry.
            val attempts = AtomicInteger(0)
            val fromRef = AtomicReference<Instant?>(null)
            val job = launch {
                cron.run { f, _ -> attempts.incrementAndGet(); fromRef.set(f); delay(10.seconds); null }
            }
            awaitUntil(timeout = 3.seconds) { attempts.get() >= 2 }
            val from = fromRef.get()!!
            job.cancelAndJoin()

            val notes = dataSource.cronTickNotesJson(prefix, name, from)
            assertTrue("timeout" in notes, "expected a timeout failure note, got: $notes")
        }
    }

    // withTimeout throws CancellationException for two distinct reasons, and run() must treat
    // them differently. Case 1: the coroutine running run() was itself cancelled from outside —
    // the exception belongs to the caller and must propagate out of run().
    @Test
    fun callerCancellationPropagatesOutOfRunAndStillRecordsTheFailure() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val theSink = sink(prefix)
            val cron = theSink.cron(name, Cron.Config(interval = 10.minutes), runImmediately = true)

            val fromRef = AtomicReference<Instant?>(null)
            val reachedCodeAfterRun = AtomicBoolean(false)
            val job = launch {
                cron.run { f, _ -> fromRef.set(f); delay(10.seconds); null }
                reachedCodeAfterRun.set(true)
            }
            awaitUntil(timeout = 3.seconds) { fromRef.get() != null }
            val from = fromRef.get()!!
            awaitUntil(timeout = 3.seconds) { dataSource.cronTickStartedAtOf(prefix, name, from) != null }
            job.cancelAndJoin()

            assertFalse(reachedCodeAfterRun.get(), "cancellation of the calling coroutine must propagate out of run()")
            assertTrue(job.isCancelled)

            val notes = dataSource.cronTickNotesJson(prefix, name, from)
            assertTrue("cancellation" in notes && "\"from\":\"parent\"" in notes, "expected a parent-cancellation note, got: $notes")
            assertEquals(null, dataSource.cronTickStartedAtOf(prefix, name, from), "the NonCancellable write-back must still have run")
        }
    }

    // Case 2: the tick lambda threw a CancellationException of its own (it awaited something from
    // an unrelated, cancelled coroutine). Our coroutine is NOT cancelled, so run() must swallow it
    // and treat it as an ordinary tick failure, then keep going.
    @Test
    fun aForeignCancellationFromTheTickIsRecordedAsAFailureAndRunKeepsGoing() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val name = uniqueQueueName()
            val theSink = sink(prefix)
            val cron = theSink.cron(name, Cron.Config(interval = 300.milliseconds, retries = 5), runImmediately = true)

            val failures = CopyOnWriteArrayList<Boolean>()
            val successes = AtomicBoolean(false)
            val runner = launch {
                cron.run(
                    onFailure = { failure -> failures += true; kotlinx.serialization.json.JsonObject(failure.defaultNotes()) },
                    onSuccess = { _, _, _ -> successes.set(true) },
                ) { _, _ ->
                    if (failures.isEmpty()) {
                        val unrelated = CompletableDeferred<Unit>()
                        unrelated.cancel()
                        unrelated.await()
                    }
                    null
                }
            }

            awaitUntil(timeout = 3.seconds) { failures.isNotEmpty() }
            assertTrue(runner.isActive, "a foreign cancellation from the tick must not take run() down")

            // The retry (same window, due immediately) succeeds, proving run() kept going.
            awaitUntil(timeout = 3.seconds) { successes.get() }
            runner.cancelAndJoin()

            assertEquals(1, failures.size, "exactly one failure should have been recorded, for the foreign cancellation")
        }
    }
}
