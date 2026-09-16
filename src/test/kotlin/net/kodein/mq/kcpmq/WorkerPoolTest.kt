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

            val poolJob = launch { q.workerPool { message -> message.successDelete() } }
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
                    message.successDelete()
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
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(messageTreatmentTimeout = 1.seconds))

            assertFailsWith<IllegalArgumentException> {
                q.workerPool(workKeepalive = 1.seconds) { }
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
                    message.successDelete()
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
                .messageQueue(queue, User.serializer(), MessageQueue.Config(messageTreatmentTimeout = 500.milliseconds))
            q.post(User(1, "slow"))

            val attempts = java.util.concurrent.atomic.AtomicInteger(0)
            val poolJob = launch {
                q.workerPool(numberOfWorkers = 1, workTimeout = 500.milliseconds) { message ->
                    if (attempts.incrementAndGet() == 1) {
                        delay(5.seconds) // exceeds workTimeout: should be requeued, not lost
                    } else {
                        message.successDelete()
                    }
                }
            }

            withTimeoutOrNullAssert(8.seconds, "the timed-out message should have been requeued and eventually reprocessed") {
                awaitUntil { attempts.get() >= 2 }
            }
            poolJob.cancelAndJoin()
        }
    }

    // [FAIL bug 4] the default workTimeout equals messageTreatmentTimeout exactly, so
    // WorkerPool.kt's `workTimeout > config.messageTreatmentTimeout` check is false and the
    // keepalive is silently disabled even though workKeepalive was computed and validated.
    @Test
    fun keepaliveIsActiveWithDefaultSettings() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val treatmentTimeout = 1.seconds
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(messageTreatmentTimeout = treatmentTimeout))
            q.post(User(1, "A"))

            var messageId: kotlin.uuid.Uuid? = null
            val poolJob = launch {
                // Default workTimeout == messageTreatmentTimeout, and a non-null workKeepalive
                // default is computed regardless — it should be active here without being asked for.
                q.workerPool { message ->
                    messageId = message.id
                    delay(2.seconds) // longer than messageTreatmentTimeout: keepalive should extend it
                    message.successDelete()
                }
            }

            awaitUntil { messageId != null }
            val before = dataSource.startedAtOf(prefix, messageId!!)
            assertNotNull(before)
            delay(1500.milliseconds) // past messageTreatmentTimeout since the message was picked up
            val after = dataSource.startedAtOf(prefix, messageId)
            assertNotNull(after)
            assertTrue(after.isAfter(before), "keepalive should be active by default and extend started_at")

            poolJob.cancelAndJoin()
        }
    }

    // [documents the escape hatch from bug 4]
    @Test
    fun keepaliveIsActiveWhenWorkTimeoutExceedsTreatmentTimeout() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val treatmentTimeout = 1.seconds
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(messageTreatmentTimeout = treatmentTimeout))
            q.post(User(1, "A"))

            var messageId: kotlin.uuid.Uuid? = null
            val poolJob = launch {
                q.workerPool(workTimeout = treatmentTimeout * 3, workKeepalive = treatmentTimeout / 3) { message ->
                    messageId = message.id
                    delay(2500.milliseconds) // longer than treatmentTimeout, shorter than workTimeout
                    message.successDelete()
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
