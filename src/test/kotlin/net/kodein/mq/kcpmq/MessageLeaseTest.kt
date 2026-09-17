package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// A message picked up but never acked (hard-crashed worker, killed process) is reclaimable once
// its lease has been expired for longer than the grace margin. Soft failures never get here —
// Message.process requeues those itself — so this covers the case where no code was left running.
class MessageLeaseTest : AbstractPgsqlTest() {

    @Test
    fun unacknowledgedMessageBecomesAvailableAgainAfterTreatmentTimeout() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(processingTimeout = 500.milliseconds))
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
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(processingTimeout = 500.milliseconds))
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
            val config = MessageQueue.Config(processingTimeout = 500.milliseconds)
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

    @Test
    fun reclaimingAnExpiredLeaseCountsAsAFailedTry() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(
                    uniqueQueueName(),
                    User.serializer(),
                    MessageQueue.Config(maxTries = 5, processingTimeout = 300.milliseconds),
                )
            q.post(User(1, "A"))
            q.tryReceive() // abandoned

            delay(1.seconds)

            val reclaimed = q.tryReceive()
            assertNotNull(reclaimed)
            assertEquals(1, reclaimed.notes.size, "the abandoned attempt must be recorded as a try")
            assertEquals(
                JsonPrimitive("leaseExpired"),
                (reclaimed.notes.single() as JsonObject)["cause"],
            )
        }
    }

    @Test
    fun aReclaimThatExhaustsMaxTriesDeadLettersTheMessage() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val observer = RecordingObserver<User>()
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(
                    queue,
                    User.serializer(),
                    MessageQueue.Config(maxTries = 1, processingTimeout = 300.milliseconds),
                    listOf(observer),
                )
            q.post(User(1, "A"))
            q.tryReceive() // abandoned, and maxTries = 1 so this was the only allowed attempt

            delay(1.seconds)

            assertNull(q.tryReceive(), "an exhausted message must not be delivered again")
            assertEquals(0, dataSource.countMessages(prefix, queue), "it must be dead-lettered, not orphaned")
            assertEquals(1, observer.failures.size)
        }
    }

    // tryReceive loops past exhausted leases, dead-lettering each, until it finds a deliverable
    // message — so a run of abandoned-and-exhausted messages must not hide the ones behind them.
    @Test
    fun tryReceiveSkipsPastSeveralExhaustedLeasesToDeliverTheNextMessage() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val observer = RecordingObserver<User>()
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(
                    queue,
                    User.serializer(),
                    MessageQueue.Config(maxTries = 1, processingTimeout = 300.milliseconds),
                    listOf(observer),
                )
            q.post(User(1, "abandoned-a"))
            q.post(User(2, "abandoned-b"))
            q.post(User(3, "deliverable"))

            // Take and abandon the first two; the third stays fresh.
            assertEquals(User(1, "abandoned-a"), q.tryReceive()?.payload)
            assertEquals(User(2, "abandoned-b"), q.tryReceive()?.payload)

            delay(1.seconds)

            val delivered = q.tryReceive()
            assertNotNull(delivered)
            assertEquals(User(3, "deliverable"), delivered.payload)
            assertEquals(2, observer.failures.size, "both exhausted leases must be dead-lettered")
            assertEquals(1, dataSource.countMessages(prefix, queue), "only the delivered message must remain")
        }
    }

    @Test
    fun extendingALeaseKeepsItFromBeingReclaimed() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(
                    uniqueQueueName(),
                    User.serializer(),
                    MessageQueue.Config(processingTimeout = 400.milliseconds),
                )
            q.post(User(1, "A"))
            val held = q.tryReceive()
            assertNotNull(held)

            // Without these the lease would be reclaimable after 400ms + the 200ms minimum grace.
            delay(300.milliseconds)
            held.extend()
            delay(300.milliseconds)
            held.extend()

            assertNull(q.tryReceive(), "an extended lease must not be reclaimable")
        }
    }

    @Test
    fun aLeaseIsNotReclaimableDuringTheGraceMargin() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(
                    uniqueQueueName(),
                    User.serializer(),
                    MessageQueue.Config(processingTimeout = 500.milliseconds, processingGrace = 1.seconds),
                )
            q.post(User(1, "A"))
            q.tryReceive() // abandoned

            delay(900.milliseconds) // past processingTimeout, still inside the grace margin
            assertNull(q.tryReceive(), "the holder must keep the lease for the whole grace margin")

            delay(900.milliseconds) // now past processingTimeout + grace
            assertNotNull(q.tryReceive())
        }
    }
}
