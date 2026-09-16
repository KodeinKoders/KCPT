package net.kodein.mq.kcpmq

import kotlinx.coroutines.test.runTest
import kotlinx.serialization.json.JsonNull
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.seconds

class MessageQueueSinkTest : AbstractPgsqlTest() {

    @Test
    fun messageQueueUsesSinkTablesPrefix() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        val mq = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(queue, User.serializer())
        mq.post(User(1, "A"))

        assertEquals(1, dataSource.countMessages(prefix, queue))
    }

    @Test
    fun sinksWithDifferentPrefixesAreFullyIsolated() = runTest(timeout = 5.seconds) {
        val queue = uniqueQueueName()
        val mqA = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix()).messageQueue(queue, User.serializer())
        val mqB = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix()).messageQueue(queue, User.serializer())

        mqA.post(User(1, "A"))

        assertNull(mqB.tryReceive())
        assertNotNull(mqA.tryReceive())
    }

    @Test
    fun queuesInSameSinkAreIsolated() = runTest(timeout = 5.seconds) {
        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix())
        val mqA = sink.messageQueue("a", User.serializer())
        val mqB = sink.messageQueue("b", User.serializer())

        mqA.post(User(1, "A"))

        assertNull(mqB.tryReceive())
        assertNotNull(mqA.tryReceive())
    }

    @Test
    fun messageQueueInitializesSchemaLazily() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        // No explicit initialize() call.
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).messageQueue(uniqueQueueName(), User.serializer())

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    kotlin.test.assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun sinkLevelObserverIsInvokedOnSuccess() = runTest(timeout = 5.seconds) {
        val observer = RecordingObserver<Any?>()
        val mq = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix(), messageObservers = listOf(observer))
            .messageQueue(uniqueQueueName(), User.serializer())
        mq.post(User(1, "A"))

        val message = mq.tryReceive()
        assertNotNull(message)
        message.successDelete()

        assertEquals(1, observer.successes.size)
        assertEquals(User(1, "A"), observer.successes[0].payload)
    }

    @Test
    fun sinkAndQueueObserversAreBothInvokedSinkFirst() = runTest(timeout = 5.seconds) {
        val order = java.util.concurrent.CopyOnWriteArrayList<String>()
        val sinkObserver = object : Message.Observer<Any?> {
            override fun onSuccess(id: kotlin.uuid.Uuid, payload: Any?, notes: kotlinx.serialization.json.JsonElement) { order += "sink" }
            override fun onFailure(id: kotlin.uuid.Uuid, payload: Any?, notes: kotlinx.serialization.json.JsonElement) {}
        }
        val queueObserver = object : Message.Observer<User> {
            override fun onSuccess(id: kotlin.uuid.Uuid, payload: User, notes: kotlinx.serialization.json.JsonElement) { order += "queue" }
            override fun onFailure(id: kotlin.uuid.Uuid, payload: User, notes: kotlinx.serialization.json.JsonElement) {}
        }
        val mq = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = uniquePrefix(), messageObservers = listOf(sinkObserver))
            .messageQueue(uniqueQueueName(), User.serializer(), messageObservers = listOf(queueObserver))
        mq.post(User(1, "A"))

        val message = mq.tryReceive()
        assertNotNull(message)
        message.successDelete(JsonNull)

        assertEquals(listOf("sink", "queue"), order)
    }

    // The prefix is interpolated into statement text as an identifier, so it is validated rather
    // than escaped — no escaping function makes an identifier safe after the fact.
    @Test
    fun tablesPrefixWithSqlMetacharactersIsRejected() = runTest(timeout = 5.seconds) {
        assertFailsWith<IllegalArgumentException> {
            MessageQueueSink(source = dataSource.asSource(), tablesPrefix = "public.mq_evil'; DROP TABLE public.mq_messages; --_").initialize()
        }
    }

    @Test
    fun anUnqualifiedTablesPrefixIsAccepted() = runTest(timeout = 5.seconds) {
        val queue = uniqueQueueName()
        val mq = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = "mq_${queue}_")
            .messageQueue(queue, User.serializer())
        mq.post(User(1, "A"))

        assertNotNull(mq.tryReceive())
    }

    @Test
    fun malformedTablesPrefixesAreRejected() = runTest(timeout = 5.seconds) {
        // Quotes, whitespace, statement separators, comment markers, too many qualifiers, and a
        // leading digit all fail the identifier shape.
        listOf(
            """mq"_""",
            "mq _",
            "mq_;drop_",
            "mq_--_",
            "a.b.c_",
            "9mq_",
            ".mq_",
        ).forEach { prefix ->
            assertFailsWith<IllegalArgumentException>("expected '$prefix' to be rejected") {
                MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
            }
        }
    }
}
