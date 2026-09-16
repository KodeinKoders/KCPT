package net.kodein.mq.kcpmq

import kotlinx.coroutines.test.runTest
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

class MessageRetryTest : AbstractPgsqlTest() {

    @Test
    fun messageIsDeliveredAtMostMaxTriesTimes() = runTest(timeout = 5.seconds) {
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
            .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(maxTries = 3))
        q.post(User(1, "A"))

        repeat(3) {
            val message = q.tryReceive()
            assertNotNull(message, "expected attempt ${it + 1} of 3 to be deliverable")
            message.failureRequeue()
        }

        assertNull(q.tryReceive(), "a 4th attempt must not be deliverable once maxTries is exhausted")
    }

    @Test
    fun exhaustedMessageIsDeletedFromTheTable() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
            .messageQueue(queue, User.serializer(), MessageQueue.Config(maxTries = 2))
        q.post(User(1, "A"))

        q.tryReceive()!!.failureRequeue()
        q.tryReceive()!!.failureRequeue()

        assertEquals(0, dataSource.countMessages(prefix, queue))
    }

    @Test
    fun exhaustedMessageNotifiesFailureObservers() = runTest(timeout = 5.seconds) {
        val observer = RecordingObserver<User>()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
            .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(maxTries = 2), listOf(observer))
        q.post(User(1, "A"))

        q.tryReceive()!!.failureRequeue()
        q.tryReceive()!!.failureRequeue()

        assertEquals(1, observer.failures.size)
        assertEquals(User(1, "A"), observer.failures.firstOrNull()?.payload)
    }

    @Test
    fun maxTriesOfOneDeadLettersOnTheFirstFailure() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
            .messageQueue(queue, User.serializer(), MessageQueue.Config(maxTries = 1))
        q.post(User(1, "A"))

        q.tryReceive()!!.failureRequeue()

        assertEquals(0, dataSource.countMessages(prefix, queue))
        assertNull(q.tryReceive())
    }

    // [FAIL bug 10] Config(maxTries = 0) silently creates a black hole: `0 < 0` is always false,
    // so tryReceive can never select a message posted to this queue in the first place, and it
    // is never cleaned up either. This should be rejected at construction time instead.
    @Test
    fun maxTriesOfZeroIsRejected() = runTest(timeout = 5.seconds) {
        assertFailsWith<IllegalArgumentException> {
            MessageQueue.Config(maxTries = 0)
        }
    }
}
