package net.kodein.mq.kcpmq

import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancel
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull
import kotlinx.serialization.Serializable
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

@Serializable
data class Foo(val bar: String)

class PGChannelTest : AbstractPgsqlTest() {

    @Test
    fun notifyIsReceivedByListener() = runTest(timeout = 5.seconds) {
        val channel = PGChannel("foos_${uniqueQueueName()}", Foo.serializer())

        launch {
            val flow = dataSource.asSource().openListenConnection(this, channel)
            val job = launch { assertEquals(Foo("bar"), flow.first()) }

            dataSource.connection.use { connection -> connection.notify(channel, Foo("bar")) }
            job.join()
            cancel()
        }.join()
    }

    @Test
    fun listenerReceivesMultipleNotificationsInOrder() = runTest(timeout = 5.seconds) {
        val channel = PGChannel("foos_${uniqueQueueName()}", Foo.serializer())

        launch {
            val flow = dataSource.asSource().openListenConnection(this, channel)
            val job = launch {
                assertEquals(listOf(Foo("a"), Foo("b"), Foo("c")), flow.take(3).toList())
            }

            dataSource.connection.use { connection ->
                connection.notify(channel, Foo("a"))
                connection.notify(channel, Foo("b"))
                connection.notify(channel, Foo("c"))
            }
            job.join()
            cancel()
        }.join()
    }

    @Test
    fun channelNameWithDotAndMixedCaseRoundTrips() = runTest(timeout = 5.seconds) {
        val channel = PGChannel("MySchema.Foos_${uniqueQueueName()}", Foo.serializer())

        launch {
            val flow = dataSource.asSource().openListenConnection(this, channel)
            val job = launch { assertEquals(Foo("bar"), flow.first()) }

            dataSource.connection.use { connection -> connection.notify(channel, Foo("bar")) }
            job.join()
            cancel()
        }.join()
    }

    @Test
    fun separateChannelsDoNotCrossTalk() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val suffix = uniqueQueueName()
            val channelA = PGChannel("a_$suffix", Foo.serializer())
            val channelB = PGChannel("b_$suffix", Foo.serializer())

            launch {
                val flowA = dataSource.asSource().openListenConnection(this, channelA)
                val job = launch {
                    val result = withTimeoutOrNull(500.milliseconds) { flowA.first() }
                    assertNull(result, "channel a must not receive a notification sent on channel b")
                }
                dataSource.connection.use { connection -> connection.notify(channelB, Foo("bar")) }
                job.join()
                cancel()
            }.join()
        }
    }

    @Test
    fun notifyIsNotDeliveredUntilTheTransactionCommits() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val channel = PGChannel("tx_${uniqueQueueName()}", Foo.serializer())

            launch {
                val flow = dataSource.asSource().openListenConnection(this, channel)
                val received = kotlinx.coroutines.CompletableDeferred<Foo>()
                val collector = launch { received.complete(flow.first()) }

                dataSource.connection.use { connection ->
                    connection.autoCommit = false
                    connection.notify(channel, Foo("bar"))

                    delay(500.milliseconds)
                    assertNull(withTimeoutOrNull(1.milliseconds) { received.await() })

                    connection.commit()
                }

                assertEquals(Foo("bar"), withTimeoutOrNull(2.seconds) { received.await() })
                collector.cancel()
                cancel()
            }.join()
        }
    }

    @Test
    fun emptyChannelNameIsRejected() {
        assertFailsWith<IllegalArgumentException> { PGChannel("", Foo.serializer()) }
    }

    @Test
    fun channelNameOf63BytesIsAcceptedAnd64BytesIsRejected() = runTest(timeout = 5.seconds) {
        val name63 = "a".repeat(63)
        val name64 = "a".repeat(64)

        val channel = PGChannel(name63, Foo.serializer())
        launch {
            val flow = dataSource.asSource().openListenConnection(this, channel)
            val job = launch { assertEquals(Foo("bar"), flow.first()) }
            dataSource.connection.use { connection -> connection.notify(channel, Foo("bar")) }
            job.join()
            cancel()
        }.join()

        assertFailsWith<IllegalArgumentException> { PGChannel(name64, Foo.serializer()) }
    }

    @Test
    fun channelNameLengthIsMeasuredInUtf8BytesNotChars() = runTest(timeout = 5.seconds) {
        // 'é' is 2 bytes in UTF-8: 31 chars = 62 bytes (ok), 32 chars = 64 bytes (rejected).
        val ok = "é".repeat(31)
        val tooLong = "é".repeat(32)

        val channel = PGChannel(ok, Foo.serializer())
        launch {
            val flow = dataSource.asSource().openListenConnection(this, channel)
            val job = launch { assertEquals(Foo("bar"), flow.first()) }
            dataSource.connection.use { connection -> connection.notify(channel, Foo("bar")) }
            job.join()
            cancel()
        }.join()

        assertFailsWith<IllegalArgumentException> { PGChannel(tooLong, Foo.serializer()) }
    }

    // Poll for the pool to be clear rather than sleeping a fixed interval and sampling once. A
    // leaked connection never comes back, so polling catches it just as surely, whereas a single
    // timed sample is a bet that the pool has settled by then — and as a bet it flaked, roughly
    // once in twenty suite runs, with no leak to show for it.
    @Test
    fun closingTheConnectionFlowReleasesTheConnection() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            val channel = PGChannel("close_${uniqueQueueName()}", Foo.serializer())
            repeat(5) {
                coroutineScope {
                    dataSource.asSource().openListenConnection(this, channel).use { }
                }
            }
            awaitUntil(
                timeout = 2.seconds,
                message = "a closed ConnectionFlow never returned its connection to the pool",
            ) { dataSource.hikariPoolMXBean.activeConnections == 0 }
        }
    }

    // Releasing the connection is not enough: it goes back to a pool, so it has to go back clean.
    // A LISTEN left registered keeps delivering into whoever borrows the connection next, which is
    // not hypothetical — a stale registration once fed a previous incarnation's notification to a
    // later listener, which took the notification for one of its own.
    @Test
    fun aReleasedConnectionIsNoLongerListening() = runTest(timeout = 8.seconds) {
        withContext(Dispatchers.Default) {
            // A pool of exactly one, so the connection borrowed below is necessarily the one that
            // was just listening.
            HikariDataSource().apply {
                jdbcUrl = pgContainer.jdbcUrl
                username = pgContainer.username
                password = pgContainer.password
                maximumPoolSize = 1
            }.use { single ->
                val channel = PGChannel("unlisten_${uniqueQueueName()}", Foo.serializer())
                coroutineScope {
                    single.asSource().openListenConnection(this, channel).closeAndJoin()
                }

                single.connection.use { connection ->
                    connection.prepareStatement(
                        "SELECT count(*) FROM pg_listening_channels() AS c WHERE c = ?"
                    ).use { statement ->
                        statement.setString(1, channel.name)
                        statement.executeQuery().use { resultSet ->
                            resultSet.next()
                            assertEquals(
                                0,
                                resultSet.getInt(1),
                                "the connection went back to the pool still listening on ${channel.name}",
                            )
                        }
                    }
                }
            }
        }
    }

    @Test
    fun closeAndJoinReleasesTheConnectionSynchronously() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val channel = PGChannel("closejoin_${uniqueQueueName()}", Foo.serializer())
            coroutineScope {
                val flow = dataSource.asSource().openListenConnection(this, channel)
                flow.closeAndJoin()
                assertEquals(0, dataSource.hikariPoolMXBean.activeConnections)
            }
        }
    }
}
