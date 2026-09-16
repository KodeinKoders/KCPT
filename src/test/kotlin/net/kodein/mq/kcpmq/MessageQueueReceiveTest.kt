package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class MessageQueueReceiveTest : AbstractPgsqlTest() {

    private suspend fun mq(prefix: String = uniquePrefix(), queue: String = uniqueQueueName(), config: MessageQueue.Config = MessageQueue.Config()) =
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer(), config)

    @Test
    fun receiveReturnsImmediatelyWhenMessageIsAlreadyAvailable() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val q = mq()
            q.post(User(1, "A"))

            val message = q.receive()
            assertEquals(User(1, "A"), message.payload)
        }
    }

    @Test
    fun receiveWakesUpWhenAMessageIsPosted() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val q = mq()
            val job = launch {
                val message = q.receive()
                assertEquals(User(1, "A"), message.payload)
            }
            delay(500.milliseconds)
            q.post(User(1, "A"))
            job.join()
        }
    }

    @Test
    fun receiveIsNotWokenByAnotherQueuesPost() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
            val a = sink.messageQueue("a", User.serializer())
            val b = sink.messageQueue("b", User.serializer())

            val job = launch { a.receive() }
            delay(500.milliseconds)
            b.post(User(1, "B"))
            delay(500.milliseconds)
            assertTrue(job.isActive, "receive() on queue a must not wake up from a post to queue b")

            a.post(User(1, "A"))
            job.join()
        }
    }

    @Test
    fun receiveWaitsUntilFutureDueAtThenReturns() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val q = mq()
            q.post(User(1, "A"), Clock.System.now() + 700.milliseconds)

            val start = Clock.System.now()
            val message = q.receive()
            val elapsed = Clock.System.now() - start

            assertEquals(User(1, "A"), message.payload)
            assertTrue(elapsed >= 600.milliseconds, "returned too early: $elapsed")
        }
    }

    @Test
    fun receiveRetargetsWakeupWhenAnEarlierDueMessageArrives() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val q = mq()
            q.post(User(1, "far"), Clock.System.now() + 5.seconds)

            val start = Clock.System.now()
            val job = launch {
                val message = q.receive()
                assertEquals(User(2, "near"), message.payload)
            }
            delay(300.milliseconds)
            q.post(User(2, "near")) // due now — should be delivered first, well before the 5s message
            job.join()
            val elapsed = Clock.System.now() - start
            assertTrue(elapsed < 2.seconds, "receive() should have been retargeted to the earlier message, took $elapsed")
        }
    }

    @Test
    fun receivePicksUpAMessageInsertedWithoutNotify() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(securityCheckTimeout = 1.seconds))
            // Bypass post()/notify entirely: a raw INSERT that KCPMQ never learns about via LISTEN.
            val id = kotlin.uuid.Uuid.random()
            dataSource.rawInsertMessage(prefix, queue, id, Clock.System.now(), """{"id":1,"name":"raw"}""")

            val message = q.receive()
            assertEquals(User(1, "raw"), message.payload)
        }
    }

    @Test
    fun receiveIsCancellableAndReleasesItsListenConnection() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val q = mq()
            val job = launch { q.receive() }
            delay(300.milliseconds)
            job.cancelAndJoin()
            delay(200.milliseconds)

            assertEquals(0, dataSource.hikariPoolMXBean.activeConnections)

            // The queue must still be usable afterwards.
            q.post(User(1, "A"))
            assertNotNull(q.tryReceive())
        }
    }

    // Uses a short securityCheckTimeout: if a given iteration loses the inherently-timed
    // LISTEN-vs-NOTIFY race (the 150ms head start below is generous but not a hard guarantee),
    // receive() should self-heal within a couple of seconds via the min(due_at) fallback rather
    // than sit on the default 10-minute securityCheckTimeout, which would look like a hang.
    @Test
    fun repeatedReceivesDoNotExhaustTheConnectionPool() = runTest(timeout = 30.seconds) {
        withContext(Dispatchers.Default) {
            val q = mq(config = MessageQueue.Config(securityCheckTimeout = 2.seconds))
            repeat(15) { i ->
                val job = launch {
                    val message = q.receive()
                    assertEquals(User(i, "u$i"), message.payload)
                    message.successDelete()
                }
                delay(150.milliseconds)
                q.post(User(i, "u$i"))
                job.join()
            }
            delay(200.milliseconds)
            assertEquals(0, dataSource.hikariPoolMXBean.activeConnections)
        }
    }

    @Test
    fun concurrentReceiversEachGetADistinctMessage() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = mq()
            val receivers = (0..3).map { async { q.receive().payload } }
            delay(300.milliseconds)
            (0..3).forEach { q.post(User(it, "u$it")) }

            val received = receivers.awaitAll()
            assertEquals((0..3).map { User(it, "u$it") }.toSet(), received.toSet())
        }
    }
}
