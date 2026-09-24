package net.kodein.kcpt

import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
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
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.SerializationException
import kotlinx.serialization.builtins.serializer
import kotlinx.serialization.encoding.Decoder
import java.util.concurrent.CopyOnWriteArrayList
import org.junit.Test
import kotlin.coroutines.cancellation.CancellationException
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.time.Instant

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

    // A notification is only ever a hint that work may be available, so one whose payload cannot be
    // read must not be propagated: letting it out failed this flow and with it every consumer
    // collecting it. NOTIFY is open to any role that can reach the database and the channel name is
    // easy to guess, so this was reachable without any privilege — and a producer on an older or
    // newer wire format would have tripped it by accident.
    @Test
    fun anUndecodableNotificationIsDroppedAndReported() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val reports = CopyOnWriteArrayList<Triple<String, String, Throwable>>()
            val name = "bad_${uniqueQueueName()}"
            val channel = PGChannel(name, Foo.serializer()) { reportedChannel, payload, cause ->
                reports += Triple(reportedChannel, payload, cause)
            }

            launch {
                val flow = dataSource.asSource().openListenConnection(this, channel)
                val received = kotlinx.coroutines.CompletableDeferred<Foo>()
                val collector = launch { received.complete(flow.first()) }
                delay(300.milliseconds) // let the collector subscribe before anything is sent

                dataSource.rawNotify(name, "not-json-at-all")
                dataSource.connection.use { connection -> connection.notify(channel, Foo("good")) }

                assertEquals(
                    Foo("good"),
                    withTimeoutOrNull(5.seconds) { received.await() },
                    "the flow must survive the undecodable notification and deliver the next one",
                )
                collector.cancel()
                cancel()
            }.join()

            assertEquals(1, reports.size, "the undecodable notification must be reported, not silently dropped")
            val (reportedChannel, payload, cause) = reports.single()
            assertEquals(name, reportedChannel)
            assertEquals("not-json-at-all", payload)
            assertTrue(cause is SerializationException, "expected the decode failure as the cause, got $cause")
        }
    }

    // Unlike the malformed-JSON case above, a validly-JSON-encoded string that isn't a valid instant
    // decodes past the JSON layer and only fails inside Instant.serializer()'s own deserialize, which
    // calls Instant.parse and throws a bare IllegalArgumentException rather than a SerializationException
    // — kcpt-message-queue's and kcpt-cron's own notification channels both use Instant.serializer(),
    // so this is the shape of payload they would actually receive from a misbehaving or hostile NOTIFY.
    @Test
    fun aValidJsonStringThatIsNotAValidInstantIsDroppedAndReported() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val reports = CopyOnWriteArrayList<Triple<String, String, Throwable>>()
            val name = "badinstant_${uniqueQueueName()}"
            val channel = PGChannel(name, Instant.serializer()) { reportedChannel, payload, cause ->
                reports += Triple(reportedChannel, payload, cause)
            }

            launch {
                val flow = dataSource.asSource().openListenConnection(this, channel)
                val received = kotlinx.coroutines.CompletableDeferred<Instant>()
                val collector = launch { received.complete(flow.first()) }
                delay(300.milliseconds) // let the collector subscribe before anything is sent

                dataSource.rawNotify(name, "\"garbage\"") // valid JSON string, not a valid Instant
                val good = Clock.System.now()
                dataSource.connection.use { connection -> connection.notify(channel, good) }

                assertEquals(
                    good,
                    withTimeoutOrNull(5.seconds) { received.await() },
                    "the flow must survive the unparseable instant and deliver the next notification",
                )
                collector.cancel()
                cancel()
            }.join()

            assertEquals(1, reports.size, "the unparseable instant must be reported, not silently dropped")
            val (reportedChannel, payload, cause) = reports.single()
            assertEquals(name, reportedChannel)
            assertEquals("\"garbage\"", payload)
            assertTrue(cause is IllegalArgumentException, "expected Instant.parse's failure as the cause, got $cause")
        }
    }

    // A genuine CancellationException surfacing from decoding must never be treated like an ordinary
    // decode failure: reporting it through onUndecodableNotification and continuing would silently
    // swallow the cancellation, the exact class of bug the CancellationException/Throwable split
    // exists to avoid. It must tear the listening loop down instead of delivering anything further.
    @Test
    fun aCancellationDuringDecodeIsNeverReportedAsUndecodable() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val reports = CopyOnWriteArrayList<Triple<String, String, Throwable>>()
            val name = "cancelduringdecode_${uniqueQueueName()}"
            val serializer = object : KSerializer<Foo> by Foo.serializer() {
                override fun deserialize(decoder: Decoder): Foo {
                    val value = Foo.serializer().deserialize(decoder)
                    if (value.bar == "cancel-me") throw CancellationException("synthetic cancellation")
                    return value
                }
            }
            val channel = PGChannel(name, serializer) { reportedChannel, payload, cause ->
                reports += Triple(reportedChannel, payload, cause)
            }

            launch {
                val flow = dataSource.asSource().openListenConnection(this, channel)
                val collector = launch {
                    assertNull(
                        withTimeoutOrNull(3.seconds) { flow.first() },
                        "a genuine CancellationException from decoding must tear the listening loop down, " +
                            "not deliver anything further",
                    )
                }
                delay(300.milliseconds)
                dataSource.connection.use { connection -> connection.notify(channel, Foo("cancel-me")) }
                collector.join()
                cancel()
            }.join()

            assertEquals(0, reports.size, "a CancellationException must never be reported via onUndecodableNotification")
        }
    }

    // The handler is caller-supplied code on the listening path, so it must not be able to fail the
    // flow either — that would re-create, by a second route, exactly the breakage it reports.
    @Test
    fun aThrowingUndecodableHandlerDoesNotBreakTheFlow() = runTest(timeout = 15.seconds) {
        withContext(Dispatchers.Default) {
            val name = "boom_${uniqueQueueName()}"
            val channel = PGChannel(name, Foo.serializer()) { _, _, _ ->
                throw RuntimeException("handler boom")
            }

            launch {
                val flow = dataSource.asSource().openListenConnection(this, channel)
                val received = kotlinx.coroutines.CompletableDeferred<Foo>()
                val collector = launch { received.complete(flow.first()) }
                delay(300.milliseconds)

                dataSource.rawNotify(name, "not-json-at-all")
                dataSource.connection.use { connection -> connection.notify(channel, Foo("good")) }

                assertEquals(
                    Foo("good"),
                    withTimeoutOrNull(5.seconds) { received.await() },
                    "a throwing handler must not take the flow down with it",
                )
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

    // workerPool calls receive() in a loop, so cancelling the pool routinely lands on a call that is
    // only just opening its listening connection. The scope passed here is deliberately not the
    // caller's, which is what makes the timing exact rather than a race to reproduce.
    @Test
    fun openListenConnectionOnAnAlreadyCancelledScopeTakesNoConnection() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val channel = PGChannel("cancelled_${uniqueQueueName()}", Foo.serializer())
            // HikariCP builds its pool on first use, and the whole point here is that nothing ever
            // asks for a connection — without this the assertion below reads a null MXBean.
            dataSource.connection.use { }
            val cancelled = CoroutineScope(Job().apply { cancel() })

            assertFailsWith<CancellationException> {
                dataSource.asSource().openListenConnection(cancelled, channel)
            }

            delay(200.milliseconds)
            assertEquals(
                0,
                dataSource.hikariPoolMXBean.activeConnections,
                "a connection was taken for a scope that was already cancelled, and never given back",
            )
        }
    }

    // The harder half: the scope is live when the connection is acquired and cancelled before the
    // teardown is armed. A coroutine cancelled before it starts never runs its body, so a teardown
    // armed after that point would never return this connection to the pool.
    @Test
    fun openListenConnectionReleasesItsConnectionWhenTheScopeDiesWhileOpening() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val channel = PGChannel("dying_${uniqueQueueName()}", Foo.serializer())
            val scope = CoroutineScope(Job())
            val underlying = dataSource.asSource()
            // Cancels the scope in the one window the production code cannot observe: after the
            // connection is handed over, before anything is watching it.
            val source: Source = { underlying().also { scope.cancel() } }

            try {
                source.openListenConnection(scope, channel)
            } catch (_: CancellationException) {
                // Either outcome is acceptable; what matters is the connection below.
            }

            delay(200.milliseconds)
            assertEquals(
                0,
                dataSource.hikariPoolMXBean.activeConnections,
                "the connection was acquired but never given back after the scope died mid-open",
            )
        }
    }
}
