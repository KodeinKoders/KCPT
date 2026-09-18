package net.kodein.mq.kcpmq

import kotlinx.coroutines.test.runTest
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toKotlinInstant

class MessageQueuePostTryReceiveTest : AbstractPgsqlTest() {

    private suspend fun mq(prefix: String = uniquePrefix(), queue: String = uniqueQueueName()) =
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())

    @Test
    fun postThenTryReceiveReturnsPayload() = runTest(timeout = 5.seconds) {
        val q = mq()
        q.post(User(42, "Salomon"))

        val message = q.tryReceive()
        assertNotNull(message)
        assertEquals(User(42, "Salomon"), message.payload)
    }

    @Test
    fun tryReceiveOnEmptyQueueReturnsNull() = runTest(timeout = 5.seconds) {
        assertNull(mq().tryReceive())
    }

    @Test
    fun tryReceiveMarksMessageStartedSoSecondTryReceiveReturnsNull() = runTest(timeout = 5.seconds) {
        val q = mq()
        q.post(User(1, "A"))

        assertNotNull(q.tryReceive())
        assertNull(q.tryReceive())
    }

    @Test
    fun postStoresPayloadAsExpectedCompactJson() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())
        q.post(User(42, "Salomon"))

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT payload FROM ${prefix}messages WHERE queue = ?").use { statement ->
                statement.setString(1, queue)
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("""{"id":42,"name":"Salomon"}""", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun postDefaultsDueAtToNow() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(uniqueQueueName(), User.serializer())
        val before = Clock.System.now()
        q.post(User(1, "A"))
        val after = Clock.System.now()

        val message = q.tryReceive()
        assertNotNull(message)
        val dueAt = dataSource.dueAtOf(prefix, message.id)!!.toInstant().toKotlinInstant()
        assertTrue(dueAt >= before - 1.seconds && dueAt <= after + 1.seconds)
    }

    @Test
    fun postWithFutureDueIsNotReceivedYet() = runTest(timeout = 5.seconds) {
        val q = mq()
        q.post(User(1, "A"), Clock.System.now() + 1.hours)

        assertNull(q.tryReceive())
    }

    @Test
    fun postWithPastDueIsReceivedImmediately() = runTest(timeout = 5.seconds) {
        val q = mq()
        q.post(User(1, "A"), Clock.System.now() - 1.hours)

        assertNotNull(q.tryReceive())
    }

    @Test
    fun postWithNearFutureDueBecomesReceivableWhenDuePasses() = runTest(timeout = 5.seconds) {
        kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) {
            val q = mq()
            q.post(User(1, "A"), Clock.System.now() + 300.milliseconds)

            assertNull(q.tryReceive())
            kotlinx.coroutines.delay(600.milliseconds)
            assertNotNull(q.tryReceive())
        }
    }

    @Test
    fun newMessageHasEmptyNotes() = runTest(timeout = 5.seconds) {
        val q = mq()
        q.post(User(1, "A"))

        val message = q.tryReceive()
        assertNotNull(message)
        assertTrue(message.notes.isEmpty())
    }

    @Test
    fun messageIdIsAUuidV7AndAscendsAcrossSequentialPosts() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        val q = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())
        q.post(User(1, "A"))
        q.post(User(2, "B"))

        val ids = dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT id FROM ${prefix}messages WHERE queue = ? ORDER BY (payload->>'id')::int").use { statement ->
                statement.setString(1, queue)
                statement.executeQuery().use { resultSet ->
                    generateSequence { if (resultSet.next()) resultSet.getObject(1) as java.util.UUID else null }.toList()
                }
            }
        }
        assertEquals(2, ids.size)
        // UUIDv7 version nibble is 7.
        ids.forEach { assertEquals(7, (it.mostSignificantBits ushr 12) and 0xF) }
        assertTrue(ids[0].toString() < ids[1].toString())
    }

    @Test
    fun tryReceiveIgnoresOtherQueuesMessages() = runTest(timeout = 5.seconds) {
        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
        val a = sink.messageQueue("a", User.serializer())
        val b = sink.messageQueue("b", User.serializer())
        b.post(User(1, "B"))

        assertNull(a.tryReceive())
        assertNotNull(b.tryReceive())
    }

    @Test
    fun messagesAreDeliveredInDueAtOrder() = runTest(timeout = 5.seconds) {
        val q = mq()
        val now = Clock.System.now()
        // Insertion order deliberately scrambled relative to due_at order.
        val dueOffsetsInInsertionOrder = listOf(3, 1, 4, 0, 2)
        for (offset in dueOffsetsInInsertionOrder) {
            q.post(User(offset, "u$offset"), now - (100 - offset).seconds)
        }

        val received = (1..5).map { q.tryReceive()!!.payload.id }
        assertEquals(listOf(0, 1, 2, 3, 4), received)
    }

    @Test
    fun messagesWithEqualDueAtAreDeliveredInPostOrder() = runTest(timeout = 5.seconds) {
        val q = mq()
        val dueAt = Clock.System.now() - 1.seconds
        val posted = (0..9).map { i -> User(i, "u$i").also { q.post(it, dueAt) } }

        val received = (1..10).map { q.tryReceive()!!.payload }
        assertEquals(posted, received)
    }

    @Test
    fun payloadWithUnknownFieldsIsDecodedLeniently() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
        val producer = sink.messageQueue(queue, UserV2.serializer())
        producer.post(UserV2(1, "A", "surplus-field-value"))

        val consumer = sink.messageQueue(queue, User.serializer())
        val message = consumer.tryReceive()
        assertNotNull(message)
        assertEquals(User(1, "A"), message.payload)
    }
}
