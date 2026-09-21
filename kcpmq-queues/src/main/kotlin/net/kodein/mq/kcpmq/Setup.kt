@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import kotlin.use


internal object Setup {
    context(tables: MQTables)
    private suspend fun initializePropertiesTable(connection: Connection) = withContext(Dispatchers.IO) {
        connection.prepareStatement("CREATE TABLE IF NOT EXISTS ${tables.properties} (key VARCHAR(32) PRIMARY KEY, value TEXT)").use {
            it.execute()
        }
    }

    // One list per version step, each entry rendering one statement from the table names.
    private val mqMigrations: List<List<MQTables.() -> String>> = listOf(
        // 0 -> 1
        listOf(
            {
                // language="PostgreSQL"
                """
                    CREATE TABLE $messages (
                        queue VARCHAR(32) NOT NULL,
                        id UUID NOT NULL,
                        due_at TIMESTAMPTZ NOT NULL,
                        started_at TIMESTAMPTZ DEFAULT NULL,
                        payload JSON NOT NULL,
                        -- NOT NULL, not merely defaulted: both selection queries filter on
                        -- json_array_length(notes) < ?, and json_array_length(NULL) is NULL,
                        -- so the predicate is neither true nor false and the row is never
                        -- selected — not for delivery, not for retry, not for dead-lettering.
                        -- It would sit in the table forever, invisible to every code path and
                        -- to maxTries. A foreign writer omitting notes from an explicit column
                        -- list is exactly how that happens, and this library expects those.
                        notes JSON NOT NULL DEFAULT '[]',
                        PRIMARY KEY(queue, id)
                    )
                """
            },
            {
                // language="PostgreSQL"
                """
                    CREATE INDEX
                    ON $messages (queue, due_at)
                    WHERE started_at IS NULL
                """
            },
        ),
    )


    // A database-wide key, so every process initialising the same tables contends on the same
    // lock. String.hashCode is specified by the JDK, so separate JVMs derive the same value.
    context(tables: MQTables)
    private fun advisoryLockKey(): Long = "kcpmq:${tables.messages}".hashCode().toLong()

    context(tables: MQTables)
    internal suspend fun initializeMQSchema(source: Source): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.transaction {
                // Reading the version and acting on it is a check-then-act across sessions, so it
                // needs to be exclusive database-wide: the per-sink init Job only covers one
                // instance. Without this, two processes starting together both read version 0 and
                // both run the migration, and the loser fails on an already-created table —
                // CREATE TABLE IF NOT EXISTS is not atomic against a concurrent creator either.
                // The lock is transaction-scoped, so it is released by the commit or rollback.
                prepareStatement("SELECT pg_advisory_xact_lock(?)").use { statement ->
                    statement.setLong(1, advisoryLockKey())
                    statement.execute()
                }

                initializePropertiesTable(connection)
                val targetVersion = mqMigrations.size
                var currentVersion = getProperty(Property.SchemaVersion) ?: 0

                // A version beyond what this build knows how to migrate to means the tables were
                // migrated by a newer release. The loop below would simply not run and report
                // success, leaving this process reading and writing a schema it does not
                // understand — surfacing later as malformed data or an unexpected SQLException on
                // the hot path, far from the cause. During a rolling deployment that is the
                // expected topology, not an edge case, so it fails here instead.
                check(currentVersion <= targetVersion) {
                    "${tables.messages} is at schema version $currentVersion, but this version of " +
                        "KCPMQ only knows up to $targetVersion: these tables have been migrated by " +
                        "a newer release and cannot be used by this one"
                }

                while (currentVersion < targetVersion) {
                    mqMigrations[currentVersion].forEach { sql ->
                        prepareStatement(sql(tables).trimIndent()).use { it.execute() }
                    }
                    ++currentVersion
                    setProperty(Property.SchemaVersion, currentVersion)
                }
            }
        }
    }
}
