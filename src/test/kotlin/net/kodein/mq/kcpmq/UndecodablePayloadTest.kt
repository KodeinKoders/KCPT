package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.json.Json
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import org.junit.Test
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

// A payload this consumer cannot decode used to throw out of tryReceive inside the transaction:
// the row rolled back untouched, stayed first in line, and took the worker pool down with it on
// every retry — a poison pill maxTries could never drain, because a message that is never leased
// never accrues a note.
class UndecodablePayloadTest : AbstractPgsqlTest() {

    private val garbage = """{"totally":"wrong"}"""

    private class RecordingUndecodable : Message.Observer<User> {
        val undecodable = CopyOnWriteArrayList<Triple<Uuid, String, JsonElement>>()
        override fun onSuccess(id: Uuid, payload: User, notes: JsonElement) {}
        override fun onFailure(id: Uuid, payload: User, notes: JsonElement) {}
        override fun onUndecodable(id: Uuid, payloadJson: String, notes: JsonElement) {
            undecodable += Triple(id, payloadJson, notes)
        }
    }

    private fun noteCount(prefix: String, id: Uuid): Int =
        (Json.parseToJsonElement(dataSource.readNotesJson(prefix, id)) as JsonArray).size

    @Test
    fun anUndecodablePayloadDoesNotBlockTheMessagesBehindIt() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(maxTries = 5))

            // Earlier due_at, so this is the row tryReceive reaches first.
            dataSource.rawInsertMessage(prefix, queue, Uuid.random(), Clock.System.now() - 1.seconds, garbage)
            q.post(User(1, "readable"))

            val received = q.tryReceive()
            assertNotNull(received, "the undecodable message must not hide the one behind it")
            assertEquals(User(1, "readable"), received.payload)
        }
    }

    @Test
    fun anUndecodablePayloadIsRetriedWithABackoffRatherThanSpinning() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(
                    queue,
                    User.serializer(),
                    MessageQueue.Config(maxTries = 5, processingTimeout = 1.seconds),
                )
            val id = Uuid.random()
            dataSource.rawInsertMessage(prefix, queue, id, Clock.System.now() - 1.seconds, garbage)

            assertNull(q.tryReceive(), "nothing else is queued, so there is nothing to deliver")
            assertEquals(1, noteCount(prefix, id), "the failed decode must be recorded as a try")

            assertNull(q.tryReceive())
            assertEquals(1, noteCount(prefix, id), "it must not be retried until the backoff elapses")
            assertEquals(1, dataSource.countMessages(prefix, queue), "it is kept for a later retry, not dropped")
        }
    }

    @Test
    fun anUndecodablePayloadIsDroppedOnceItRunsOutOfTries() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val observer = RecordingUndecodable()
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(
                    queue,
                    User.serializer(),
                    MessageQueue.Config(maxTries = 2, processingTimeout = 200.milliseconds),
                    listOf(observer),
                )
            val id = Uuid.random()
            dataSource.rawInsertMessage(prefix, queue, id, Clock.System.now() - 1.seconds, garbage)

            assertNull(q.tryReceive())
            assertEquals(1, noteCount(prefix, id))

            delay(400.milliseconds) // past the pushed-out due_at
            assertNull(q.tryReceive())

            assertEquals(0, dataSource.countMessages(prefix, queue), "it must be dropped once maxTries is spent")
            assertEquals(1, observer.undecodable.size, "observers must be told, with the raw payload")
            assertEquals(id, observer.undecodable.single().first)
            assertEquals(garbage, observer.undecodable.single().second)
        }
    }

    // Rolling the message forward makes it available again at a new due_at, so it announces itself
    // like every other write that does. The consumer that performed the roll-forward loops on
    // immediately and needs no telling; one parked on a later deadline does. Asserted on the
    // channel directly, so it does not rest on another consumer's timing.
    @OptIn(kotlin.time.ExperimentalTime::class)
    @Test
    fun rollingAnUndecodablePayloadForwardAnnouncesTheNewDueAt() = runTest(timeout = 20.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(
                    queue,
                    User.serializer(),
                    MessageQueue.Config(maxTries = 5, processingTimeout = 2.seconds),
                )
            dataSource.rawInsertMessage(prefix, queue, Uuid.random(), Clock.System.now() - 1.seconds, garbage)

            val channel = PGChannel("${prefix}messages_$queue", kotlin.time.Instant.serializer())
            launch {
                val flow = dataSource.asSource().openListenConnection(this, channel)
                val announced = kotlinx.coroutines.CompletableDeferred<kotlin.time.Instant>()
                val collector = launch { announced.complete(flow.first()) }
                delay(300.milliseconds) // let the collector subscribe before anything is sent

                assertNull(q.tryReceive(), "nothing else is queued, so there is nothing to deliver")

                assertNotNull(
                    withTimeoutOrNull(5.seconds) { announced.await() },
                    "the roll-forward must announce the message's new due_at on the queue's channel",
                )
                collector.cancel()
                cancel()
            }.join()
        }
    }

    @Test
    fun anUndecodablePayloadDoesNotKillAWorkerPool() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(
                    queue,
                    User.serializer(),
                    MessageQueue.Config(maxTries = 5, processingTimeout = 300.milliseconds),
                )
            dataSource.rawInsertMessage(prefix, queue, Uuid.random(), Clock.System.now() - 1.seconds, garbage)

            val processed = CopyOnWriteArrayList<User>()
            val poolJob = launch {
                q.workerPool(numberOfWorkers = 1) { message -> processed += message.payload; null }
            }
            q.post(User(1, "readable"))

            awaitUntil { processed.isNotEmpty() }
            assertTrue(poolJob.isActive, "the pool must survive a message it cannot decode")
            assertEquals(listOf(User(1, "readable")), processed.toList())

            poolJob.cancel()
        }
    }
}
