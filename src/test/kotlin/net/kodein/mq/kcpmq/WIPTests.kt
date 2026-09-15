@file:Suppress("SqlResolve")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.serialization.Serializable
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.seconds


@Serializable
data class Foo(val bar: String)

class ChannelsTests : AbstractPgsqlTest() {

    @Test
    fun testPublishAndReceive(): Unit = runTest(timeout = 5.seconds) {
        val channel = PGChannel<Foo>("myschema.foos", Foo.serializer(), dataSource.asSource())

        launch {
            val flow = channel.openListenConnection(this)
            val job = launch {
                assertEquals(Foo("bar"), flow.first())
            }

            channel.notify(Foo("bar"))
            job.join()

            cancel()
        }.join()
    }

    @Serializable
    data class User(
        val id: Int,
        val name: String,
    )

    @Test
    fun testSendMessage() = runTest(timeout = 30.seconds) {
        val manager = MessageQueueSink(
            source = dataSource.asSource(),
        )
        val mq = manager.messageQueue("test", User.serializer())
        mq.post(User(42, "Salomon"))

        val message = mq.tryReceive()
        assertNotNull(message)
        assertEquals(User(42, "Salomon"), message.payload)
        assertNull(mq.tryReceive())
        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT * FROM public.mq_messages").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    val json = resultSet.getString(resultSet.findColumn("payload"))
                    assertEquals("""{"id":42,"name":"Salomon"}""", json)
                }
            }
        }

        message.successDelete()
        assertNull(mq.tryReceive())
        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT * FROM public.mq_messages").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertFalse(resultSet.next())
                }
            }
        }
    }

    @Test
    fun testFutureMessageIsNotReceived() = runTest(timeout = 30.seconds) {
        val manager = MessageQueueSink(
            source = dataSource.asSource(),
        )
        val mq = manager.messageQueue("test", User.serializer())
        mq.post(User(42, "Salomon"), Clock.System.now() + 1.hours)

        assertNull(mq.tryReceive())
    }
}
