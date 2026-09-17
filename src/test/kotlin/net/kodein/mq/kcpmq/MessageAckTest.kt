package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class MessageAckTest : AbstractPgsqlTest() {

    private suspend fun mqWithObserver(config: MessageQueue.Config = MessageQueue.Config()): Pair<MessageQueue<User>, RecordingObserver<User>> {
        val observer = RecordingObserver<User>()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
            .messageQueue(uniqueQueueName(), User.serializer(), config, listOf(observer))
        return q to observer
    }

    @Test
    fun successDeleteRemovesTheRow() = runTest(timeout = 5.seconds) {
        val (q, _) = mqWithObserver()
        q.post(User(1, "A"))
        val message = q.tryReceive()!!
        message.successDelete()

        assertNull(q.tryReceive())
    }

    @Test
    fun successDeleteNotifiesOnSuccessWithNotes() = runTest(timeout = 5.seconds) {
        val (q, observer) = mqWithObserver()
        q.post(User(1, "A"))
        val message = q.tryReceive()!!
        message.successDelete(JsonPrimitive("done"))

        assertEquals(1, observer.successes.size)
        assertEquals(User(1, "A"), observer.successes[0].payload)
        assertEquals(JsonPrimitive("done"), observer.successes[0].notes)
        assertTrue(observer.failures.isEmpty())
    }

    @Test
    fun failureDeleteRemovesTheRow() = runTest(timeout = 5.seconds) {
        val (q, _) = mqWithObserver()
        q.post(User(1, "A"))
        val message = q.tryReceive()!!
        message.failureDelete()

        assertNull(q.tryReceive())
    }

    @Test
    fun failureDeleteNotifiesOnFailureWithNotes() = runTest(timeout = 5.seconds) {
        val (q, observer) = mqWithObserver()
        q.post(User(1, "A"))
        val message = q.tryReceive()!!
        message.failureDelete(JsonPrimitive("bad"))

        assertEquals(1, observer.failures.size)
        assertEquals(User(1, "A"), observer.failures[0].payload)
        assertEquals(JsonPrimitive("bad"), observer.failures[0].notes)
        assertTrue(observer.successes.isEmpty())
    }

    @Test
    fun successDeleteOnlyAffectsItsOwnMessage() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())
        q.post(User(1, "A"))
        q.post(User(2, "B"))
        val first = q.tryReceive()!!
        first.successDelete()

        val remaining = q.tryReceive()
        assertNotNull(remaining)
        assertEquals(1, dataSource.countMessages(prefix, queue))
    }

    @Test
    fun failureRequeueMakesMessageReceivableAgain() = runTest(timeout = 5.seconds) {
        val (q, _) = mqWithObserver(MessageQueue.Config(maxTries = 5))
        q.post(User(1, "A"))
        val message = q.tryReceive()!!
        message.failureRequeue()

        val redelivered = q.tryReceive()
        assertNotNull(redelivered)
        assertEquals(message.id, redelivered.id)
    }

    @Test
    fun failureRequeueAppendsTheNote() = runTest(timeout = 5.seconds) {
        val (q, _) = mqWithObserver(MessageQueue.Config(maxTries = 5))
        q.post(User(1, "A"))
        val message = q.tryReceive()!!
        message.failureRequeue(notes = JsonPrimitive("attempt-1-failed"))

        val redelivered = q.tryReceive()!!
        assertEquals(listOf(JsonPrimitive("attempt-1-failed")), redelivered.notes)
    }

    @Test
    fun failureRequeueAccumulatesNotesAcrossAttempts() = runTest(timeout = 5.seconds) {
        val (q, _) = mqWithObserver(MessageQueue.Config(maxTries = 5))
        q.post(User(1, "A"))
        q.tryReceive()!!.failureRequeue(notes = JsonPrimitive("n1"))
        q.tryReceive()!!.failureRequeue(notes = JsonPrimitive("n2"))
        val third = q.tryReceive()!!

        assertEquals(listOf(JsonPrimitive("n1"), JsonPrimitive("n2")), third.notes)
    }

    @Test
    fun failureRequeueWithFutureDueDelaysRedelivery() = runTest(timeout = 5.seconds) {
        val (q, _) = mqWithObserver(MessageQueue.Config(maxTries = 5))
        q.post(User(1, "A"))
        val message = q.tryReceive()!!
        message.failureRequeue(due = Clock.System.now() + 1.hours)

        assertNull(q.tryReceive())
    }

    @Test
    fun failureRequeueWithNullDueRequeuesForNow() = runTest(timeout = 5.seconds) {
        val (q, _) = mqWithObserver(MessageQueue.Config(maxTries = 5))
        q.post(User(1, "A"))
        q.tryReceive()!!.failureRequeue(due = null)

        assertNotNull(q.tryReceive())
    }

    @Test
    fun failureRequeueDoesNotNotifyObservers() = runTest(timeout = 5.seconds) {
        val (q, observer) = mqWithObserver(MessageQueue.Config(maxTries = 5))
        q.post(User(1, "A"))
        q.tryReceive()!!.failureRequeue()

        assertTrue(observer.successes.isEmpty())
        assertTrue(observer.failures.isEmpty())
    }

    // A consumer already parked in receive() has no way to notice that a message became available
    // again: it computed its wake-up deadline while the message was still leased. Both fallbacks
    // are set far beyond this test's patience, so the only thing that can deliver the message
    // inside the budget is the notification the requeue itself sends. Without it the retry waited
    // out the lease deadline — and before the lease deadline existed at all, the security timer,
    // ten minutes by default.
    @Test
    fun aParkedReceiveWakesWhenAMessageIsRequeued() = runTest(timeout = 20.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
                .messageQueue(
                    uniqueQueueName(),
                    User.serializer(),
                    MessageQueue.Config(
                        maxTries = 5,
                        processingTimeout = 60.seconds,    // lease deadline a minute-plus away
                        securityCheckTimeout = 60.seconds, // and the security timer no nearer
                    ),
                )
            q.post(User(1, "A"))
            val held = q.tryReceive()
            assertNotNull(held)

            val waiter = async { withTimeoutOrNull(5.seconds) { q.receive() } }
            delay(500.milliseconds) // parked, with nothing else in the queue that could wake it

            held.failureRequeue()

            val redelivered = waiter.await()
            assertNotNull(
                redelivered,
                "a parked receive() must wake on the requeue, not sit out the lease deadline",
            )
            assertEquals(held.id, redelivered.id)
        }
    }

    @Test
    fun extendAdvancesStartedAt() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(uniqueQueueName(), User.serializer())
        q.post(User(1, "A"))
        val message = q.tryReceive()!!
        val before = dataSource.startedAtOf(prefix, message.id)
        assertNotNull(before)

        kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) {
            kotlinx.coroutines.delay(20.milliseconds)
        }
        message.extend()
        val after = dataSource.startedAtOf(prefix, message.id)

        assertNotNull(after)
        assertTrue(after.isAfter(before))
    }
}
