package net.kodein.mq.kcpmq

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
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
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'schema_version'").use { statement ->
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
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'schema_version'").use { statement ->
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
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'schema_version'").use { statement ->
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
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    // [FAIL bug 8] Two independent MessageQueueSink instances racing the DDL migration: Setup.kt's
    // CREATE TABLE (no IF NOT EXISTS, no advisory lock) is not safe under concurrent first-time init.
    // A start-gate barrier plus 16 concurrent sinks makes this fail the large majority of the time,
    // but it is a genuine timing race and may occasionally pass — that is expected for this class
    // of bug and is not a sign the test itself is wrong.
    @Test
    fun concurrentInitializeAcrossIndependentSinksSucceeds() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val sinks = (1..16).map { MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix) }
        val gate = kotlinx.coroutines.CompletableDeferred<Unit>()

        val jobs = sinks.map { sink -> async { gate.await(); sink.initialize() } }
        gate.complete(Unit)
        jobs.awaitAll()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    // [FAIL bug 9] MessageQueueSink.kt's fast path calls Job.join(), which does not rethrow — a
    // failed first migration should not be cached as a successful one.
    @Test
    fun failedInitializeIsNotCachedAsSuccess() = runTest(timeout = 5.seconds) {
        // A schema that does not exist and cannot be auto-created: migration must fail every time.
        val prefix = "missing_schema_${uniquePrefix().substringAfter("public.")}.mq_"
        val sink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)

        assertFailsWith<Exception> { sink.initialize() }
        assertFailsWith<Exception> { sink.initialize() }
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
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }
}
