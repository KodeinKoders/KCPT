package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Test
import java.util.concurrent.ConcurrentLinkedQueue
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class WorkerPoolTest : AbstractPgsqlTest() {

    @Test
    fun workerPoolProcessesAllPostedMessages() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())
            (0..9).forEach { q.post(User(it, "u$it")) }

            val poolJob = launch { q.workerPool { null } }
            awaitUntil { dataSource.countMessages(prefix, queue) == 0 }
            poolJob.cancelAndJoin()
        }
    }

    @Test
    fun workerPoolRespectsNumberOfWorkersAsAConcurrencyCeiling() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix()).messageQueue(uniqueQueueName(), User.serializer())
            (0..9).forEach { q.post(User(it, "u$it")) }

            val inFlight = java.util.concurrent.atomic.AtomicInteger(0)
            var maxInFlight = 0
            val releaseGate = kotlinx.coroutines.sync.Mutex(locked = true)

            val poolJob = launch {
                q.workerPool(numberOfWorkers = 3) { message ->
                    val current = inFlight.incrementAndGet()
                    synchronized(this) { maxInFlight = maxOf(maxInFlight, current) }
                    releaseGate.withLock { }
                    inFlight.decrementAndGet()
                    null
                }
            }

            delay(500.milliseconds) // let workers pile up against the gate
            assertEquals(3, inFlight.get(), "expected exactly numberOfWorkers messages in flight")
            releaseGate.unlock()

            poolJob.cancelAndJoin()
            assertEquals(3, maxInFlight)
        }
    }

    @Test
    fun workerPoolRejectsKeepaliveNotLessThanTreatmentTimeout() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(processingTimeout = 1.seconds))

            assertFailsWith<IllegalArgumentException> {
                q.workerPool(keepalive = 1.seconds) { null }
            }
        }
    }

    // [FAIL bug 5] workerPool has no catch around work(), only finally — one throwing handler
    // cancels the whole pool. Other already-posted messages should still be processed.
    @Test
    fun workerPoolSurvivesAThrowingWorkLambda() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())
            q.post(User(1, "throws"))
            q.post(User(2, "ok-a"))
            q.post(User(3, "ok-b"))

            val processed = ConcurrentLinkedQueue<Int>()
            val poolJob = launch {
                q.workerPool(numberOfWorkers = 1) { message ->
                    if (message.payload.name == "throws") {
                        message.successDelete()
                        throw RuntimeException("boom")
                    }
                    processed += message.payload.id
                    null
                }
            }

            withTimeoutOrNullAssert(5.seconds, "workerPool must keep running after one handler throws") {
                awaitUntil { processed.containsAll(listOf(2, 3)) }
            }
            poolJob.cancelAndJoin()
        }
    }

    // [FAIL bug 6] withTimeoutOrNull returning null in workMessage does nothing — the message is
    // neither acked nor requeued, it is simply lost.
    @Test
    fun workerPoolRequeuesAMessageThatExceedsWorkTimeout() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(processingTimeout = 500.milliseconds))
            q.post(User(1, "slow"))

            val attempts = java.util.concurrent.atomic.AtomicInteger(0)
            val poolJob = launch {
                q.workerPool(numberOfWorkers = 1, timeout = 500.milliseconds) { message ->
                    if (attempts.incrementAndGet() == 1) {
                        delay(5.seconds) // exceeds workTimeout: should be requeued, not lost
                    }
                    null
                }
            }

            withTimeoutOrNullAssert(8.seconds, "the timed-out message should have been requeued and eventually reprocessed") {
                awaitUntil { attempts.get() >= 2 }
            }
            poolJob.cancelAndJoin()
        }
    }

    // Under default settings no keepalive is needed, and none is configured: workerPool's timeout
    // equals the queue's processingTimeout, so the holder's own withTimeout fires and hands the
    // message back a whole processingGrace before anyone could reclaim it. The message is therefore
    // requeued by its own worker — and then redelivered, promptly, since a parked receive() wakes on
    // the lease deadline — but it is never extended, and never worked by two workers at once.
    //
    // started_at cannot tell those apart: it advances on an extension AND on a redelivery. The
    // notes can — extend() never writes them, so a note is proof the message was handed back rather
    // than held on to.
    @Test
    fun workThatOverrunsIsRequeuedRatherThanExtendedUnderDefaultSettings() = runTest(timeout = 20.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(processingTimeout = 1.seconds))
            q.post(User(1, "A"))

            var messageId: kotlin.uuid.Uuid? = null
            val attempts = java.util.concurrent.atomic.AtomicInteger(0)
            val inFlight = java.util.concurrent.atomic.AtomicInteger(0)
            val maxInFlight = java.util.concurrent.atomic.AtomicInteger(0)

            val poolJob = launch {
                q.workerPool { message ->
                    messageId = message.id
                    attempts.incrementAndGet()
                    val current = inFlight.incrementAndGet()
                    maxInFlight.updateAndGet { maxOf(it, current) }
                    try {
                        delay(5.seconds) // overruns processingTimeout, so this attempt is timed out
                        null
                    } finally {
                        // In a finally, so a timed-out attempt is counted out again. Below the
                        // delay it would never run, and the counter would tally attempts rather
                        // than concurrency.
                        inFlight.decrementAndGet()
                    }
                }
            }

            // The first attempt overran and was handed back; a second one means it was redelivered
            // rather than stranded.
            awaitUntil(timeout = 10.seconds) { attempts.get() >= 2 }
            poolJob.cancelAndJoin()

            assertEquals(1, maxInFlight.get(), "the message was worked by two workers at once")

            val notes = Json.parseToJsonElement(dataSource.readNotesJson(prefix, messageId!!)) as JsonArray
            assertTrue(notes.isNotEmpty(), "the overrun must be recorded as a failed try")
            assertEquals(
                JsonPrimitive("timeout"),
                (notes.first() as JsonObject)["cause"],
                "the holder's own timeout must be what handed the message back — a reclaim would " +
                    "read leaseExpired, and an extension would have written no note at all",
            )
        }
    }

    // The path where a keepalive genuinely is needed: an explicit timeout longer than the lease.
    @Test
    fun keepaliveIsActiveWhenWorkTimeoutExceedsTreatmentTimeout() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val treatmentTimeout = 1.seconds
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(processingTimeout = treatmentTimeout))
            q.post(User(1, "A"))

            var messageId: kotlin.uuid.Uuid? = null
            val poolJob = launch {
                q.workerPool(timeout = treatmentTimeout * 3, keepalive = treatmentTimeout / 3) { message ->
                    messageId = message.id
                    delay(2500.milliseconds) // longer than treatmentTimeout, shorter than workTimeout
                    null
                }
            }

            awaitUntil { messageId != null }
            val before = dataSource.startedAtOf(prefix, messageId!!)
            assertNotNull(before)
            delay(1800.milliseconds) // well past treatmentTimeout
            val after = dataSource.startedAtOf(prefix, messageId)
            assertNotNull(after)
            assertTrue(after.isAfter(before), "keepalive should have extended started_at past the treatment timeout")

            poolJob.cancelAndJoin()
        }
    }

    private suspend fun <T> withTimeoutOrNullAssert(timeout: kotlin.time.Duration, message: String, block: suspend () -> T): T {
        return kotlinx.coroutines.withTimeoutOrNull(timeout) { block() } ?: throw AssertionError(message)
    }
}
