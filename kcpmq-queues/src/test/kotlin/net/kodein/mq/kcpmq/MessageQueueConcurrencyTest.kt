package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class MessageQueueConcurrencyTest : AbstractPgsqlTest() {

    override val poolSize = 16

    @Test
    fun concurrentTryReceiveDeliversASingleMessageExactlyOnce() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix()).messageQueue(uniqueQueueName(), User.serializer())
            q.post(User(1, "A"))

            val results = (1..8).map { async { q.tryReceive() } }.awaitAll()

            assertEquals(1, results.count { it != null })
        }
    }

    @Test
    fun concurrentTryReceiveDistributes20MessagesWithoutDuplicates() = runTest(timeout = 10.seconds) {
        withContext(Dispatchers.Default) {
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix()).messageQueue(uniqueQueueName(), User.serializer())
            (0..19).forEach { q.post(User(it, "u$it")) }

            val drainers = (1..6).map {
                async {
                    val collected = mutableListOf<Int>()
                    while (true) {
                        val message = q.tryReceive() ?: break
                        collected += message.payload.id
                    }
                    collected
                }
            }
            val allIds = drainers.awaitAll().flatten()

            assertEquals((0..19).toList(), allIds.sorted())
        }
    }

    @Test
    fun concurrentPostsAreAllPersisted() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())

            (0..19).map { async { q.post(User(it, "u$it")) } }.awaitAll()

            assertEquals(20, dataSource.countMessages(prefix, queue))
        }
    }

    @Test
    fun tryReceiveSkipsRowsLockedByAnotherTransaction() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val queue = uniqueQueueName()
            val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())
            q.post(User(1, "A"))

            val lockHeld = kotlinx.coroutines.CompletableDeferred<Unit>()
            val releaseLock = kotlinx.coroutines.CompletableDeferred<Unit>()
            val lockJob = async {
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

            val start = kotlin.time.Clock.System.now()
            val result = q.tryReceive()
            val elapsed = kotlin.time.Clock.System.now() - start

            assertNull(result, "the row is locked by another transaction, so it must be skipped, not returned")
            assertTrue(elapsed < 1.seconds, "tryReceive() must not block on the other transaction's row lock, took $elapsed")

            releaseLock.complete(Unit)
            lockJob.await()
            assertNotNull(q.tryReceive(), "once the lock is released, the message should be receivable")
        }
    }
}
