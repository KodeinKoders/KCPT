package net.kodein.kcpt.mq

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.newSingleThreadContext
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.serialization.json.JsonPrimitive
import net.kodein.kcpt.AbstractPgsqlTest
import net.kodein.kcpt.Source
import org.junit.Test
import java.lang.reflect.InvocationHandler
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.PreparedStatement
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

// Source only moves connection acquisition to Dispatchers.IO, so it is easy for the blocking
// statement itself to end up on the caller's dispatcher — a core-count-limited pool it can starve,
// or a single thread it can freeze. Driving the queue from a named single-threaded dispatcher makes
// that directly observable: nothing blocking may run on that thread.
@OptIn(DelicateCoroutinesApi::class)
class BlockingJdbcDispatcherTest : AbstractPgsqlTest() {

    private val callerThread = "kcpmq-test-caller"

    // Wraps a real connection, recording the thread each statement actually executes on.
    private fun recordingSource(executedOn: MutableList<String>): Source = {
        val real = withContext(Dispatchers.IO) { dataSource.connection }
        Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java),
        ) { _, method, args ->
            val result = if (args == null) method.invoke(real) else method.invoke(real, *args)
            if (method.name == "prepareStatement") {
                Proxy.newProxyInstance(
                    PreparedStatement::class.java.classLoader,
                    arrayOf(PreparedStatement::class.java),
                ) { _, sm, sargs ->
                    if (sm.name == "executeUpdate" || sm.name == "executeQuery" || sm.name == "execute") {
                        executedOn += Thread.currentThread().name
                    }
                    if (sargs == null) sm.invoke(result) else sm.invoke(result, *sargs)
                }
            } else result
        } as Connection
    }

    private suspend fun queue(executedOn: MutableList<String>): MessageQueue<User> =
        MessageQueueSink(source = recordingSource(executedOn), tablesPrefix = uniquePrefix())
            .messageQueue(uniqueQueueName(), User.serializer(), MessageQueue.Config(maxTries = 5))

    private suspend fun message(executedOn: MutableList<String>): Message<User> {
        val mq = queue(executedOn)
        mq.post(User(1, "A"))
        return mq.tryReceive()!!
    }

    private fun assertNothingRanOnTheCallerThread(executedOn: List<String>) {
        assertTrue(executedOn.isNotEmpty(), "no statement was recorded, so the test proves nothing")
        assertEquals(
            emptyList(),
            executedOn.filter { it == callerThread },
            "blocking JDBC must not run on the caller's dispatcher; ran on $executedOn",
        )
    }

    @Test
    fun successDeleteDoesNotBlockTheCallersThread() = runTest(timeout = 15.seconds) {
        newSingleThreadContext(callerThread).use { caller ->
            val executedOn = CopyOnWriteArrayList<String>()
            withContext(caller) {
                val message = message(executedOn)
                executedOn.clear()
                message.successDelete()
            }
            assertNothingRanOnTheCallerThread(executedOn)
        }
    }

    @Test
    fun failureDeleteDoesNotBlockTheCallersThread() = runTest(timeout = 15.seconds) {
        newSingleThreadContext(callerThread).use { caller ->
            val executedOn = CopyOnWriteArrayList<String>()
            withContext(caller) {
                val message = message(executedOn)
                executedOn.clear()
                message.failureDelete(JsonPrimitive("bad"))
            }
            assertNothingRanOnTheCallerThread(executedOn)
        }
    }

    @Test
    fun failureRequeueDoesNotBlockTheCallersThread() = runTest(timeout = 15.seconds) {
        newSingleThreadContext(callerThread).use { caller ->
            val executedOn = CopyOnWriteArrayList<String>()
            withContext(caller) {
                val message = message(executedOn)
                executedOn.clear()
                message.failureRequeue(notes = JsonPrimitive("retry"))
            }
            assertNothingRanOnTheCallerThread(executedOn)
        }
    }

    @Test
    fun extendDoesNotBlockTheCallersThread() = runTest(timeout = 15.seconds) {
        newSingleThreadContext(callerThread).use { caller ->
            val executedOn = CopyOnWriteArrayList<String>()
            withContext(caller) {
                val message = message(executedOn)
                executedOn.clear()
                message.extend()
            }
            assertNothingRanOnTheCallerThread(executedOn)
        }
    }

    // A message is posted first, so receive() takes it on its first pass and never reaches the
    // wait loop — what is under test is its database work, not its parking.
    @Test
    fun receiveDoesNotBlockTheCallersThread() = runTest(timeout = 15.seconds) {
        newSingleThreadContext(callerThread).use { caller ->
            val executedOn = CopyOnWriteArrayList<String>()
            withContext(caller) {
                val mq = queue(executedOn)
                mq.post(User(1, "A"))
                executedOn.clear()
                mq.receive()
            }
            assertNothingRanOnTheCallerThread(executedOn)
        }
    }

    @Test
    fun postAndTryReceiveDoNotBlockTheCallersThread() = runTest(timeout = 15.seconds) {
        newSingleThreadContext(callerThread).use { caller ->
            val executedOn = CopyOnWriteArrayList<String>()
            withContext(caller) { message(executedOn) }
            assertNothingRanOnTheCallerThread(executedOn)
        }
    }
}
