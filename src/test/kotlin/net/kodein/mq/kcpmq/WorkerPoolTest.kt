package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
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

    // Under default settings no keepalive is needed, and none is configured: timeout equals
    // processingTimeout, so the worker's own withTimeout fires and requeues the message a whole
    // processingGrace before a reclaim could take it. Work that overruns is therefore handed back
    // rather than extended, and never processed twice.
    @Test
    fun workThatOverrunsIsRequeuedRatherThanExtendedUnderDefaultSettings() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(processingTimeout = 500.milliseconds))
            q.post(User(1, "A"))

            var messageId: kotlin.uuid.Uuid? = null
            val concurrentlyInFlight = java.util.concurrent.atomic.AtomicInteger(0)
            var maxConcurrentlyInFlight = 0
            val poolJob = launch {
                q.workerPool { message ->
                    messageId = message.id
                    val inFlight = concurrentlyInFlight.incrementAndGet()
                    synchronized(this@WorkerPoolTest) {
                        maxConcurrentlyInFlight = maxOf(maxConcurrentlyInFlight, inFlight)
                    }
                    delay(2.seconds) // overruns processingTimeout
                    concurrentlyInFlight.decrementAndGet()
                    null
                }
            }

            awaitUntil { messageId != null }
            val startedAtWhenPickedUp = dataSource.startedAtOf(prefix, messageId!!)
            assertNotNull(startedAtWhenPickedUp)

            // Past processingTimeout: the lease is not being extended, because the worker's own
            // timeout is what handles the overrun.
            delay(1200.milliseconds)
            val startedAtLater = dataSource.startedAtOf(prefix, messageId)
            if (startedAtLater != null) {
                assertEquals(
                    startedAtWhenPickedUp,
                    startedAtLater,
                    "no keepalive is configured by default, so started_at must not be extended",
                )
            }

            // And the overrun never results in the same message being worked twice at once.
            assertEquals(1, maxConcurrentlyInFlight)

            poolJob.cancelAndJoin()
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
