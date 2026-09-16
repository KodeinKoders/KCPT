package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// [FAIL bug 3] messageTreatmentTimeout never appears in any SQL predicate — started_at is only
// ever compared with IS NULL, so a message that was picked up but never acked (crashed worker,
// hung worker, forgotten ack) is lost forever. Every test in this class documents that gap and
// is expected to fail against the current implementation.
class MessageLeaseTest : AbstractPgsqlTest() {

    @Test
    fun unacknowledgedMessageBecomesAvailableAgainAfterTreatmentTimeout() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(messageTreatmentTimeout = 500.milliseconds))
            q.post(User(1, "A"))

            val firstAttempt = q.tryReceive()
            assertNotNull(firstAttempt)
            // Deliberately never ack firstAttempt — simulate a crashed/hung worker.

            delay(1500.milliseconds) // well past the 500ms treatment timeout

            val secondAttempt = q.tryReceive()
            assertNotNull(secondAttempt, "an abandoned lease should have been reclaimed")
            assertEquals(firstAttempt.id, secondAttempt.id)
        }
    }

    @Test
    fun receiveReclaimsAnExpiredLease() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(messageTreatmentTimeout = 500.milliseconds))
            q.post(User(1, "A"))
            q.tryReceive() // pick it up and abandon it

            delay(1500.milliseconds)

            val reclaimed = q.receive()
            assertEquals(User(1, "A"), reclaimed.payload)
        }
    }

    @Test
    fun crashedWorkerMessageIsRedeliveredToAnotherConsumer() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
            val queue = uniqueQueueName()
            val config = MessageQueue.Config(messageTreatmentTimeout = 500.milliseconds)
            val consumerA = sink.messageQueue(queue, User.serializer(), config)
            val consumerB = sink.messageQueue(queue, User.serializer(), config)
            consumerA.post(User(1, "A"))

            val pickedUpByA = consumerA.tryReceive()
            assertNotNull(pickedUpByA)
            // consumerA "crashes" here — never acks.

            delay(1500.milliseconds)

            val pickedUpByB = consumerB.tryReceive()
            assertNotNull(pickedUpByB, "consumer B should have been able to reclaim A's abandoned message")
            assertEquals(pickedUpByA.id, pickedUpByB.id)
        }
    }
}
