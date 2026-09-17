package net.kodein.mq.kcpmq

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import org.junit.Test
import java.util.concurrent.atomic.AtomicInteger
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toKotlinInstant

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

    // A message that is due but row-locked by another transaction is invisible to tryReceive, which
    // skips locked rows. receive() must park until something changes rather than retrying in a tight
    // loop: it derives its wake-up deadline only from messages due in the strict future, so a
    // due-but-untakeable row can never yield a deadline that has already passed. Deriving it from
    // messages due *now* let the wait loop fall through and the retry loop spin on the database
    // instead — thousands of connections and round trips a second, per waiting consumer.
    @Test
    fun receiveDoesNotSpinWhileADueMessageIsLockedByAnotherTransaction() = runTest(timeout = 30.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()

            // Counting wrapper: every database round trip in receive() starts with one acquisition.
            val acquisitions = AtomicInteger(0)
            val underlying = dataSource.asSource()
            val countingSource: Source = { acquisitions.incrementAndGet(); underlying() }

            val q = MessageQueueSink(source = countingSource, tablesPrefix = prefix)
                .messageQueue(queue, User.serializer(), MessageQueue.Config(securityCheckTimeout = 30.seconds))
            q.post(User(1, "A"))

            val lockHeld = CompletableDeferred<Unit>()
            val releaseLock = CompletableDeferred<Unit>()
            val lockJob = launch {
                dataSource.connection.use { connection ->
                    connection.autoCommit = false
                    connection.prepareStatement("SELECT * FROM ${prefix}messages WHERE queue = ? FOR UPDATE").use { statement ->
                        statement.setString(1, queue)
                        statement.executeQuery().use { it.next() }
                    }
                    lockHeld.complete(Unit)
                    releaseLock.await()
                    connection.rollback()
                }
            }
            lockHeld.await()

            acquisitions.set(0)
            val waiter = launch { withTimeoutOrNull(2.seconds) { q.receive() } }
            delay(2.seconds)
            val acquired = acquisitions.get()
            waiter.join()

            // A parked receive() needs a couple of connections in total; the spin used ~4400 a second.
            assertTrue(
                acquired < 50,
                "receive() spun on the database while the only due message was locked: " +
                    "$acquired connections acquired in 2s",
            )

            releaseLock.complete(Unit)
            lockJob.join()

            // And it is still healthy: once the lock goes, the message is delivered.
            assertNotNull(q.tryReceive(), "the message should be receivable once the lock is released")
        }
    }

    // A message's lease starts when receive() takes it, not when the caller gets it back — so
    // whatever receive() spends between those two points is spent out of the message's processing
    // budget. A receive() that parked has a listening connection to tear down first, and that
    // teardown waits for the notification poll to come round. It has to stay well under the
    // smallest permitted processingGrace: when it did not, a queue with a short processingTimeout
    // had its whole margin consumed here, and messages were reclaimed and worked a second time
    // while their first worker was still going on to succeed.
    @Test
    fun receiveReturnsAMessagePromptlyAfterTakingItsLease() = runTest(timeout = 20.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val q = mq(prefix = prefix, config = MessageQueue.Config(processingTimeout = 10.seconds))

            repeat(3) { i ->
                val waiter = async { q.receive() to Clock.System.now() }
                delay(400.milliseconds) // let it park, so the listening flow is actually collected
                q.post(User(i, "u$i"))

                val (message, returnedAt) = waiter.await()
                val leasedAt = dataSource.startedAtOf(prefix, message.id)!!.toInstant().toKotlinInstant()
                val handover = returnedAt - leasedAt

                assertTrue(
                    handover < MessageQueue.Config.MIN_PROCESSING_GRACE,
                    "receive() held the message for $handover after leasing it, which does not fit " +
                        "in the smallest permitted processingGrace of ${MessageQueue.Config.MIN_PROCESSING_GRACE}",
                )
                message.successDelete()
            }
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
