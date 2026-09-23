package net.kodein.kcpt.mq

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
import net.kodein.kcpt.AbstractPgsqlTest
import net.kodein.kcpt.asSource
import org.junit.Test
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
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

    // An observer runs on the acknowledgement path, after the row is already gone, and
    // successDelete is called from inside process()'s try — so an observer that throws used to be
    // indistinguishable from a processing failure. process() would then requeue a message it had
    // just deleted, and at maxTries = 1 that requeue dead-letters: the same message reported as
    // both a success and a failure, to an observer that never even saw the success.
    @Test
    fun aThrowingObserverDoesNotTurnASuccessIntoAFailure() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val throwing = ThrowingObserver<User>(onSuccess = true)
            val watching = RecordingObserver<User>()
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val mq = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(
                    queue,
                    User.serializer(),
                    MessageQueue.Config(maxTries = 1),
                    listOf(throwing, watching),
                )
            mq.post(User(1, "A"))

            mq.tryReceive()!!.process { null }

            assertEquals(listOf("success"), throwing.calls, "the throwing observer must see only the success")
            assertEquals(1, watching.successes.size, "an observer behind a throwing one must still be told")
            assertTrue(
                watching.failures.isEmpty(),
                "a message that succeeded must not also be reported as a failure",
            )
            assertEquals(0, dataSource.countMessages(prefix, queue), "the message succeeded and is gone")
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

    // failureRequeue itself can throw (a dropped connection, an exhausted pool). Unguarded, that
    // exception used to leave process() from inside this very catch block.
    @Test
    fun processDoesNotThrowWhenTheRequeueFails() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val mq = MessageQueueSink(source = dataSource.asFailureRequeueFailingOnceSource(), tablesPrefix = prefix)
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(maxTries = 5))
            mq.post(User(1, "A"))
            val message = mq.tryReceive()!!

            val reported = mutableListOf<Pair<MessageProcessingFailure, Throwable>>()
            message.process(onRequeueFailure = { _, failure, cause -> reported += failure to cause }) {
                throw RuntimeException("boom")
            }

            assertEquals(1, reported.size, "the failed requeue must be reported exactly once")
            assertTrue(reported[0].first is MessageProcessingFailure.Exception, "the original failure must still be reported")
            assertTrue(reported[0].second is java.sql.SQLException, "the requeue's own failure must be reported as the cause")
        }
    }

    // The regression this guards: containing the requeue's failure must not also contain the
    // caller's own cancellation. ensureActive() runs after the guard, not inside it.
    @Test
    fun callerCancellationStillPropagatesWhenTheRequeueFails() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val mq = MessageQueueSink(source = dataSource.asFailureRequeueFailingOnceSource(), tablesPrefix = prefix)
                .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(maxTries = 5))
            mq.post(User(1, "A"))
            val message = mq.tryReceive()!!

            val reachedCodeAfterProcess = AtomicBoolean(false)
            var reportedCause: Throwable? = null
            val job = launch {
                message.process(
                    timeout = 10.seconds,
                    onRequeueFailure = { _, _, cause -> reportedCause = cause },
                ) { delay(10.seconds); null }
                reachedCodeAfterProcess.set(true)
            }
            delay(300.milliseconds)
            job.cancelAndJoin()

            assertFalse(
                reachedCodeAfterProcess.get(),
                "cancellation of the calling coroutine must propagate out of process() even when the requeue fails",
            )
            assertTrue(job.isCancelled)
            assertNotNull(reportedCause, "the failed requeue must still be reported")
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

    // A timeout longer than processingTimeout used to need a caller-supplied keepalive, with two
    // ways to get the pairing wrong — both refused by a require(). There is no longer a keepalive
    // parameter to misconfigure: process() decides for itself, from timeout and the message's own
    // stamped lease, whether one is needed, so a long timeout on its own now just works.
    @Test
    fun aTimeoutLongerThanProcessingTimeoutKeepsTheLeaseAliveAutomatically() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(processingTimeout = 1.seconds))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!
            val before = dataSource.startedAtOf(f.prefix, message.id)
            assertNotNull(before)

            val job = launch {
                // Longer than processingTimeout, so the automatic keepalive (timeout > lease) is
                // active; well under timeout, so it is not what ends the call.
                message.process(timeout = 5.seconds) {
                    delay(1500.milliseconds)
                    null
                }
            }

            delay(1200.milliseconds) // past processingTimeout — reclaimable by now without a keepalive
            val after = dataSource.startedAtOf(f.prefix, message.id)
            assertNotNull(after)
            assertTrue(after.isAfter(before), "the lease should have been extended automatically")

            job.join()
            assertEquals(0, dataSource.countMessages(f.prefix, f.queue), "the message should have been processed exactly once, successfully")
        }
    }

    // timeout > lease alone isn't the only trigger: a handover slower than processingGrace — the
    // delay between receiving this message and process() actually starting, exactly the gap
    // workerPool's own acquire-then-dispatch ordering is meant to bound — can eat enough of the
    // margin that even a *fixed* timeout equal to processingTimeout (workerPool's own default,
    // rather than process()'s adaptive expiresAt-now() one) would otherwise run past the reclaim
    // threshold. The keepalive must still fire in that case.
    @Test
    fun aHandoverDelayPastProcessingGraceTriggersKeepaliveEvenAtDefaultTimeout() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(processingTimeout = 1.seconds)) // processingGrace = 200ms
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            delay(900.milliseconds) // the handover delay itself, well past processingGrace
            val before = dataSource.startedAtOf(f.prefix, message.id)
            assertNotNull(before)

            val job = launch {
                // Fixed at processingTimeout, like workerPool's own default — not process()'s
                // adaptive expiresAt-now() default, which would already price the delay in.
                message.process(timeout = 1.seconds) {
                    delay(600.milliseconds)
                    null
                }
            }

            delay(500.milliseconds) // 900+500=1400ms since receipt, past processingTimeout+processingGrace=1200ms
            val after = dataSource.startedAtOf(f.prefix, message.id)
            assertNotNull(after)
            assertTrue(after.isAfter(before), "the lease should have been kept alive despite the handover delay")

            job.join()
            assertEquals(0, dataSource.countMessages(f.prefix, f.queue))
        }
    }

    // A handler must never even be attempted once its lease has already nominally expired by the
    // time process() starts — running it at that point would be racing whichever consumer may
    // already be reclaiming the message, not genuinely holding it.
    @Test
    fun processThrowsIfTheLeaseHasAlreadyExpiredBeforeItEvenStarts() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(processingTimeout = 300.milliseconds))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            delay(400.milliseconds) // past expiresAt already

            assertFailsWith<IllegalStateException> {
                message.process { null }
            }
        }
    }

    // The bound is the lease as *stamped* (expiresAt - receivedAt), not the lease as it stands right
    // now (expiresAt - Clock.System.now()). The difference only shows once time has passed since the
    // receive: process()'s own default timeout was computed from a now() taken a moment before the
    // check runs, so a now()-based bound would be fractionally smaller than the default it is meant
    // to admit, and the defaults would reject themselves.
    @Test
    fun theDefaultTimeoutIsStillAcceptedAfterTimeHasPassedSinceTheReceive() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val f = fixture(MessageQueue.Config(processingTimeout = 1.minutes))
            f.mq.post(User(1, "A"))
            val message = f.mq.tryReceive()!!

            delay(500.milliseconds) // the lease is now visibly shorter than it was at receipt

            message.process { null }

            assertEquals(0, dataSource.countMessages(f.prefix, f.queue))
        }
    }
}
