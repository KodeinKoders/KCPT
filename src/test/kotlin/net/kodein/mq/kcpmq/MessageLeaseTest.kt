package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Clock
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

    // The dead-letter notification for an exhausted lease happens inside receiveNextOrNull's scan,
    // after its commit. A throwing observer there used to be rethrown by the transaction wrapper,
    // abandoning the scan mid-loop: the deletes were already durable, but the deliverable message
    // behind them was never returned, and tryReceive failed outright.
    @Test
    fun aThrowingObserverDoesNotAbandonTheDeadLetterScan() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val throwing = ThrowingObserver<User>(onFailure = true)
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(
                    queue,
                    User.serializer(),
                    MessageQueue.Config(maxTries = 1, processingTimeout = 300.milliseconds),
                    listOf(throwing),
                )
            q.post(User(1, "abandoned-a"))
            q.post(User(2, "abandoned-b"))
            q.post(User(3, "deliverable"))

            // Take and abandon the first two; the third stays fresh.
            assertEquals(User(1, "abandoned-a"), q.tryReceive()?.payload)
            assertEquals(User(2, "abandoned-b"), q.tryReceive()?.payload)

            delay(1.seconds)

            val delivered = q.tryReceive()
            assertNotNull(delivered, "a throwing observer must not abandon the scan behind it")
            assertEquals(User(3, "deliverable"), delivered.payload)
            assertEquals(
                listOf("failure", "failure"),
                throwing.calls,
                "both exhausted leases must still be reported, despite the first one throwing",
            )
        }
    }

    // A consumer that is already parked inside receive() must wake when an abandoned lease becomes
    // reclaimable. Its wake-up deadline used to be derived from unleased messages only, so a live
    // lease was invisible to it: the message became reclaimable a grace margin after its holder's
    // timeout, but nobody looked again until the securityCheckTimeout — ten minutes by default —
    // which reduced the whole lease mechanism to that timer. The distinction matters because this
    // is the ordinary shape of a worker pool: workers sit in receive() while other workers work.
    @Test
    fun aParkedReceiveWakesWhenAnAbandonedLeaseBecomesReclaimable() = runTest(timeout = 20.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(
                    uniqueQueueName(),
                    User.serializer(),
                    // A securityCheckTimeout far beyond the test's patience: if the deadline falls
                    // back to it, the receive() below returns nothing and the test fails.
                    MessageQueue.Config(processingTimeout = 500.milliseconds, securityCheckTimeout = 60.seconds),
                )
            q.post(User(1, "A"))
            val abandoned = q.tryReceive()
            assertNotNull(abandoned) // leased, and deliberately never acknowledged

            val reclaimed = withTimeoutOrNull(5.seconds) { q.receive() }

            assertNotNull(
                reclaimed,
                "a parked receive() must wake on the lease deadline, not sit on the securityCheckTimeout",
            )
            assertEquals(abandoned.id, reclaimed.id)
            assertEquals(
                JsonPrimitive("leaseExpired"),
                (reclaimed.notes.single() as JsonObject)["cause"],
                "the abandoned attempt must be recorded as a try",
            )
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

    // The reclaim deadline is processingTimeout + processingGrace, so a message whose timeout has
    // passed but whose grace has not must stay with its holder. Both cases are set up by backdating
    // started_at rather than by sleeping: the original slept 900ms into a 1.5s window, which left
    // 600ms of slack for a delay that can only ever overrun, and under a loaded machine it did —
    // about once in twenty suite runs. Backdating puts seconds of slack on both sides and takes no
    // wall-clock time at all.
    @Test
    fun aLeaseIsNotReclaimableDuringTheGraceMargin() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val config = MessageQueue.Config(processingTimeout = 1.seconds, processingGrace = 10.seconds)
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(uniqueQueueName(), User.serializer(), config)
            q.post(User(1, "A"))

            val held = q.tryReceive()
            assertNotNull(held) // picked up, and deliberately never acknowledged
            val leaseWindow = config.processingTimeout + config.processingGrace // 11s

            // Well past processingTimeout, well short of the end of the grace margin.
            dataSource.setStartedAt(prefix, held.id, Clock.System.now() - config.processingTimeout - 5.seconds)
            assertNull(q.tryReceive(), "the holder must keep the lease for the whole grace margin")

            // Well past the end of the grace margin.
            dataSource.setStartedAt(prefix, held.id, Clock.System.now() - leaseWindow - 5.seconds)
            val reclaimed = q.tryReceive()
            assertNotNull(reclaimed, "the lease must be reclaimable once the grace margin has passed")
            assertEquals(held.id, reclaimed.id)
        }
    }
}
