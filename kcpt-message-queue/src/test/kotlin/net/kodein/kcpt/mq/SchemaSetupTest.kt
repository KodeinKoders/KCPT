@file:OptIn(InternalKcptApi::class)

package net.kodein.kcpt.mq

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.test.runTest
import net.kodein.kcpt.AbstractPgsqlTest
import net.kodein.kcpt.InternalKcptApi
import net.kodein.kcpt.Property
import net.kodein.kcpt.asSource
import net.kodein.kcpt.getProperty
import net.kodein.kcpt.rawSetProperty
import net.kodein.kcpt.setProperty
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class SchemaSetupTest : AbstractPgsqlTest() {

    override val poolSize = 20

    @Test
    fun initializeCreatesPropertiesAndMessagesTables() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? ORDER BY column_name"
            ).use { statement ->
                statement.setString(1, prefix.removePrefix("public.") + "messages")
                statement.executeQuery().use { resultSet ->
                    val columns = generateSequence { if (resultSet.next()) resultSet.getString(1) else null }.toSet()
                    assertEquals(setOf("due_at", "id", "notes", "payload", "queue", "started_at"), columns)
                }
            }
        }
    }

    @Test
    fun initializeRecordsSchemaVersion() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'message_queue_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun initializeCreatesPartialDueAtIndex() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT count(*) FROM pg_indexes WHERE schemaname = 'public' AND tablename = ? AND indexdef LIKE '%due_at%' AND indexdef LIKE '%started_at%'"
            ).use { statement ->
                statement.setString(1, prefix.removePrefix("public.") + "messages")
                statement.executeQuery().use { resultSet ->
                    resultSet.next()
                    assertTrue(resultSet.getInt(1) >= 1)
                }
            }
        }
    }

    @Test
    fun initializeTwiceOnSameSinkIsIdempotent() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
        sink.initialize()
        sink.initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'message_queue_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun initializeIsIdempotentAcrossSequentialSinkInstances() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'message_queue_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun concurrentInitializeOnSameSinkRunsMigrationOnce() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)

        (1..8).map { async { sink.initialize() } }.awaitAll()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'message_queue_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun concurrentInitializeAcrossIndependentSinksSucceeds() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val sinks = (1..16).map { MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix) }
        val gate = kotlinx.coroutines.CompletableDeferred<Unit>()

        val jobs = sinks.map { sink -> async { gate.await(); sink.initialize() } }
        gate.complete(Unit)
        jobs.awaitAll()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'message_queue_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun failedInitializeIsNotCachedAsSuccess() = runTest(timeout = 5.seconds) {
        // A schema that does not exist and cannot be auto-created: migration must fail every time.
        val prefix = "missing_schema_${uniquePrefix().substringAfter("public.")}.mq_"
        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)

        assertFailsWith<Exception> { sink.initialize() }
        assertFailsWith<Exception> { sink.initialize() }
    }

    // A failure must not be cached either way: not as a success, and not as a permanent failure —
    // one transient outage would otherwise poison the sink for the life of the process.
    @Test
    fun initializeRetriesAfterAFailure() = runTest(timeout = 5.seconds) {
        val schema = "late_${uniquePrefix().substringAfter("public.").trimEnd('_')}"
        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = "$schema.mq_")

        assertFailsWith<Exception> { sink.initialize() }

        // The cause is now fixed, exactly as an operator restoring a database would.
        dataSource.connection.use { connection ->
            connection.createStatement().use { it.execute("CREATE SCHEMA $schema") }
        }

        sink.initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM $schema.mq_properties WHERE key = 'message_queue_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    // Initialisation is shared, so it must not belong to whichever caller reached it first.
    @Test
    fun cancellingTheFirstCallerDoesNotBreakInitialisationForOthers() = runTest(timeout = 5.seconds) {
        kotlinx.coroutines.withContext(kotlinx.coroutines.Dispatchers.Default) {
            val prefix = uniquePrefix()
            val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)

            val firstCaller = launch { sink.initialize() }
            firstCaller.cancel()
            firstCaller.join()

            // A second caller must still end up with a usable schema.
            sink.initialize()
            val mq = sink.messageQueue(uniqueQueueName(), User.serializer())
            mq.post(User(1, "A"))
            assertNotNull(mq.tryReceive())
        }
    }

    @Test
    fun initializationScopeEagerlyCreatesSchema() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        kotlinx.coroutines.coroutineScope {
            MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix, initializationScope = this)
            // Let the eagerly-launched migration job run to completion before checking.
            kotlinx.coroutines.yield()
        }

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'message_queue_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    // setProperty is an upsert, but initializeMQSchema only ever reaches its insert: with a single
    // migration it writes message_queue_schema_version exactly once per fresh database, so the ON
    // CONFLICT branch is unreachable through any public path. Exercised directly here so that
    // whoever adds a second migration is not the first to find out whether it works.
    @Test
    fun setPropertyOverwritesAnExistingValue() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        val tables = MessageQueueTables.named(
            properties = "${prefix}properties",
            messages = "${prefix}messages",
        )
        val probe = Property("d5_probe", Property.Encoder.StringEncoder)

        dataSource.asSource()().use { connection ->
            context(tables) {
                connection.setProperty(probe, "first")
                assertEquals("first", connection.getProperty(probe))

                connection.setProperty(probe, "second")
                assertEquals("second", connection.getProperty(probe), "the second write must replace the first")
            }
        }
    }

    // A corrupted or hand-edited message_queue_schema_version must not read as "not yet migrated"
    // — that treats a data problem as an ordinary fresh database and re-runs the migration against
    // tables that already exist, surfacing as "CREATE TABLE ... already exists" rather than naming
    // the actual cause.
    @Test
    fun anUnreadableSchemaVersionFailsInitializationRatherThanReMigrating() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.rawSetProperty(prefix, "message_queue_schema_version", "not-a-number")

        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
        val failure = assertFailsWith<IllegalStateException> { sink.initialize() }
        assertTrue(
            "not-a-number" in (failure.message ?: ""),
            "the failure must name the offending value; message was: ${failure.message}",
        )
    }
}
