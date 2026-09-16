package net.kodein.mq.kcpmq

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.Json
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.Uuid

class MessageProcessTest : AbstractPgsqlTest() {

    private class Fixture(
        val prefix: String,
        val queue: String,
        val mq: MessageQueue<User>,
    )

    private suspend fun fixture(config: MessageQueue.Config = MessageQueue.Config()): Fixture {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        val mq = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
            .messageQueue(queue, User.serializer(), config)
        return Fixture(prefix, queue, mq)
    }

    private fun lastNote(prefix: String, id: Uuid): JsonObject? =
        (Json.parseToJsonElement(dataSource.readNotesJson(prefix, id)) as JsonArray).lastOrNull() as? JsonObject

    private fun causeOf(prefix: String, id: Uuid): String? =
        (lastNote(prefix, id)?.get("cause") as? JsonPrimitive)?.content

    private fun fromOf(prefix: String, id: Uuid): String? =
        (lastNote(prefix, id)?.get("from") as? JsonPrimitive)?.content

    @Test
    fun processDeletesTheMessageWhenTheLambdaReturns() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture()
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            message.process { null }

            assertEquals(0, dataSource.countMessages(f.prefix, f.queue))
        }
    }

    @Test
    fun processStoresTheNotesReturnedByTheLambdaOnSuccess() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val observer = RecordingObserver<User>()
            val prefix = uniquePrefix()
            val mq = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(), listOf(observer))
            mq.post(User(1, "A"))
            val message = mq.tryReceive()!!

            message.process { JsonPrimitive("all good") }

            assertEquals(1, observer.successes.size)
            assertEquals(JsonPrimitive("all good"), observer.successes[0].notes)
        }
    }

    @Test
    fun processRequeuesWhenTheLambdaThrows() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(maxTries = 5))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            message.process { throw RuntimeException("boom") }

            assertEquals("exception", causeOf(f.prefix, message.id))
            val redelivered = f.mq.tryReceive()
            assertNotNull(redelivered)
            assertEquals(message.id, redelivered.id)
        }
    }

    @Test
    fun processRequeuesWhenTheLambdaExceedsTheTimeout() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(maxTries = 5))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            message.process(timeout = 300.milliseconds) { delay(10.seconds); null }

            assertEquals("timeout", causeOf(f.prefix, message.id))
            val redelivered = f.mq.tryReceive()
            assertNotNull(redelivered)
            assertEquals(message.id, redelivered.id)
        }
    }

    // withTimeout throws CancellationException for two distinct reasons, and process() must treat
    // them differently. Case 1: the coroutine running process() was itself cancelled — the
    // exception belongs to us and must propagate.
    @Test
    fun processRethrowsWhenTheCallingCoroutineIsCancelled() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(maxTries = 5))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            val reachedCodeAfterProcess = AtomicBoolean(false)
            val job = launch {
                message.process(timeout = 10.seconds) { delay(10.seconds); null }
                reachedCodeAfterProcess.set(true)
            }
            delay(300.milliseconds)
            job.cancelAndJoin()

            assertFalse(
                reachedCodeAfterProcess.get(),
                "cancellation of the calling coroutine must propagate out of process()"
            )
            assertTrue(job.isCancelled)
        }
    }

    // Case 2: the process lambda threw a CancellationException of its own (it awaited something
    // from an unrelated, cancelled coroutine). Our coroutine is NOT cancelled, so the exception
    // must be swallowed and treated as an ordinary processing failure.
    @Test
    fun processDoesNotRethrowWhenTheLambdaThrowsAForeignCancellation() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(maxTries = 5))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            val reachedCodeAfterProcess = AtomicBoolean(false)
            val job = launch {
                message.process(timeout = 10.seconds) {
                    // An unrelated coroutine's cancellation surfacing inside our lambda.
                    val unrelated = CompletableDeferred<Unit>()
                    unrelated.cancel()
                    unrelated.await()
                    null
                }
                reachedCodeAfterProcess.set(true)
            }
            job.join()

            assertTrue(
                reachedCodeAfterProcess.get(),
                "a CancellationException from the lambda must not propagate out of process()"
            )
            assertFalse(job.isCancelled, "the calling coroutine must stay healthy")
        }
    }

    @Test
    fun processRequeuesTheMessageWhenTheCallingCoroutineIsCancelled() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(maxTries = 5))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            val job = launch { message.process(timeout = 10.seconds) { delay(10.seconds); null } }
            delay(300.milliseconds)
            job.cancelAndJoin()

            // The NonCancellable requeue must still have run despite the cancellation.
            assertEquals("cancellation", causeOf(f.prefix, message.id))
            assertNull(dataSource.startedAtOf(f.prefix, message.id), "message must be released for redelivery")
            val redelivered = f.mq.tryReceive()
            assertNotNull(redelivered)
            assertEquals(message.id, redelivered.id)
        }
    }

    @Test
    fun processRequeuesTheMessageWhenTheLambdaThrowsAForeignCancellation() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(maxTries = 5))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            message.process(timeout = 10.seconds) {
                val unrelated = CompletableDeferred<Unit>()
                unrelated.cancel()
                unrelated.await()
                null
            }

            assertEquals("cancellation", causeOf(f.prefix, message.id))
            val redelivered = f.mq.tryReceive()
            assertNotNull(redelivered)
            assertEquals(message.id, redelivered.id)
        }
    }

    @Test
    fun processRecordsWhetherTheCancellationCameFromTheParentOrTheWork() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val parent = fixture(MessageQueue.Config(maxTries = 5))
            parent.mq.post(User(1, "A"))
            val parentMessage = parent.mq.tryReceive()!!
            val job = launch { parentMessage.process(timeout = 10.seconds) { delay(10.seconds); null } }
            delay(300.milliseconds)
            job.cancelAndJoin()

            val work = fixture(MessageQueue.Config(maxTries = 5))
            work.mq.post(User(2, "B"))
            val workMessage = work.mq.tryReceive()!!
            workMessage.process(timeout = 10.seconds) {
                val unrelated = CompletableDeferred<Unit>()
                unrelated.cancel()
                unrelated.await()
                null
            }

            assertEquals("parent", fromOf(parent.prefix, parentMessage.id))
            assertEquals("work", fromOf(work.prefix, workMessage.id))
        }
    }
}
