package net.kodein.kcpt.cron

import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.test.runTest
import net.kodein.kcpt.AbstractPgsqlTest
import net.kodein.kcpt.asSource
import net.kodein.kcpt.rawSetProperty
import net.kodein.kcpt.mq.MessageQueueSink
import net.kodein.kcpt.mq.User
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class CronSchemaSetupTest : AbstractPgsqlTest() {

    override val poolSize = 20

    private fun defaultConfig() = Cron.Config(interval = 1.minutes)

    @Test
    fun initializeCreatesCronsCursorTable() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        CronSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? ORDER BY column_name"
            ).use { statement ->
                statement.setString(1, prefix.removePrefix("public.") + "crons")
                statement.executeQuery().use { resultSet ->
                    val columns = generateSequence { if (resultSet.next()) resultSet.getString(1) else null }.toSet()
                    assertEquals(setOf("name", "next_slot_at"), columns)
                }
            }
        }
    }

    @Test
    fun initializeCreatesCronTicksTable() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        CronSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = ? ORDER BY column_name"
            ).use { statement ->
                statement.setString(1, prefix.removePrefix("public.") + "cron_ticks")
                statement.executeQuery().use { resultSet ->
                    val columns = generateSequence { if (resultSet.next()) resultSet.getString(1) else null }.toSet()
                    assertEquals(setOf("from_at", "name", "notes", "started_at", "to_at"), columns)
                }
            }
        }
    }

    @Test
    fun initializeRecordsCronSchemaVersion() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        CronSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'cron_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun initializeTwiceOnSameSinkIsIdempotent() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
        sink.initialize()
        sink.initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'cron_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    @Test
    fun concurrentInitializeAcrossIndependentCronSinksRunsMigrationOnce() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val sinks = (1..8).map { CronSink(source = dataSource.asSource(), tablesPrefix = prefix) }

        sinks.map { async { it.initialize() } }.awaitAll()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT value FROM ${prefix}properties WHERE key = 'cron_schema_version'").use { statement ->
                statement.executeQuery().use { resultSet ->
                    assertTrue(resultSet.next())
                    assertEquals("1", resultSet.getString(1))
                }
            }
        }
    }

    // Same rationale as the messages-side anUnreadableSchemaVersionFailsInitializationRatherThan
    // ReMigrating: a corrupted cron_schema_version must fail loudly rather than being collapsed to
    // "not yet migrated" and re-running CREATE TABLE against a table that already exists.
    @Test
    fun anUnreadableCronSchemaVersionFailsInitializationRatherThanReMigrating() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        CronSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.rawSetProperty(prefix, "cron_schema_version", "not-a-number")

        val sink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)
        val failure = assertFailsWith<IllegalStateException> { sink.initialize() }
        assertTrue(
            "not-a-number" in (failure.message ?: ""),
            "the failure must name the offending value; message was: ${failure.message}",
        )
    }

    // The independence this whole design is about: each sink creates only its own table, and a
    // corrupted counter on one side must not affect the other's ability to initialize.
    @Test
    fun aCronSinkAloneNeverCreatesTheMessagesTable() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        CronSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            connection.prepareStatement(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?"
            ).use { statement ->
                statement.setString(1, prefix.removePrefix("public.") + "messages")
                statement.executeQuery().use { resultSet ->
                    resultSet.next()
                    assertEquals(0, resultSet.getInt(1), "a CronSink must never create the messages table")
                }
            }
        }
    }

    @Test
    fun aMessageQueueSinkAloneNeverCreatesTheCronsTables() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.connection.use { connection ->
            for (table in listOf("crons", "cron_ticks")) {
                connection.prepareStatement(
                    "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public' AND table_name = ?"
                ).use { statement ->
                    statement.setString(1, prefix.removePrefix("public.") + table)
                    statement.executeQuery().use { resultSet ->
                        resultSet.next()
                        assertEquals(0, resultSet.getInt(1), "a MessageQueueSink must never create the $table table")
                    }
                }
            }
        }
    }

    @Test
    fun aMessageQueueSinkAndACronSinkOnTheSamePrefixInitializeIndependently() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val messageQueueSink = MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix)
        val cronSink = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)

        listOf(async { messageQueueSink.initialize() }, async { cronSink.initialize() }).awaitAll()

        dataSource.connection.use { connection ->
            connection.prepareStatement("SELECT key, value FROM ${prefix}properties WHERE key IN (?, ?)").use { statement ->
                statement.setString(1, "message_queue_schema_version")
                statement.setString(2, "cron_schema_version")
                statement.executeQuery().use { resultSet ->
                    val versions = generateSequence {
                        if (resultSet.next()) resultSet.getString("key") to resultSet.getString("value") else null
                    }.toMap()
                    assertEquals(mapOf("message_queue_schema_version" to "1", "cron_schema_version" to "1"), versions)
                }
            }
        }

        // Both tables exist, each created by its own sink.
        cronSink.cron("probe", defaultConfig())
        val mq = messageQueueSink.messageQueue(uniqueQueueName(), User.serializer())
        mq.post(User(1, "A"))
    }
}
