@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.kcpt.cron

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import net.kodein.kcpt.Property
import net.kodein.kcpt.Source
import net.kodein.kcpt.getProperty
import net.kodein.kcpt.setProperty
import net.kodein.kcpt.transaction
import kotlin.use


// Its own version, independent of MessageQueueSetup's: the two feature modules migrate on their
// own schedule, and this is the property key that tracks this module's alone.
internal val cronSchemaVersion = Property("cron_schema_version", Property.Encoder.IntEncoder)

internal object CronSetup {
    // 0 -> 1
    private val cronMigrations: List<List<CronTables.() -> String>> = listOf(
        listOf(
            {
                // The cursor: purely "how far has materialization gotten". Never touched by tick
                // processing — a tick's own success/failure/reclaim only ever writes to $cronTicks.
                // language="PostgreSQL"
                """
                    CREATE TABLE $crons (
                        name VARCHAR(32) PRIMARY KEY,
                        next_slot_at TIMESTAMPTZ NOT NULL
                    )
                """
            },
            {
                // One row per outstanding tick, materialized once its window has fully elapsed and
                // deleted once it succeeds or is dead-lettered. Shaped like `messages`, but with no
                // due_at: a row is only ever created already-due, so there is nothing to schedule
                // ahead of time — the whole reason it exists is that its window is already over.
                // language="PostgreSQL"
                """
                    CREATE TABLE $cronTicks (
                        name VARCHAR(32) NOT NULL,
                        from_at TIMESTAMPTZ NOT NULL,
                        to_at TIMESTAMPTZ NOT NULL,
                        started_at TIMESTAMPTZ DEFAULT NULL,
                        notes JSON NOT NULL DEFAULT '[]',
                        PRIMARY KEY (name, from_at)
                    )
                """
            },
            {
                // Serves the leasing SELECT's `ORDER BY from_at LIMIT 1` for a not-yet-leased tick —
                // the hot path, run every time a server looks for work. Mirrors messages' own
                // (queue, due_at) index (MessageQueueSetup.kt): name leads for its equality, from_at
                // gives the ordering, and started_at IS NULL matches the same half of the leasing
                // predicate this index can serve (the disjunction's other half cannot be indexed,
                // same reasoning as messages).
                // language="PostgreSQL"
                """
                    CREATE INDEX
                    ON $cronTicks (name, from_at)
                    WHERE started_at IS NULL
                """
            },
            {
                // Serves the wake-up query's other half: min(started_at) WHERE name = ? AND
                // started_at IS NOT NULL, asked whenever every existing row for this name is
                // currently leased. Mirrors messages' partial index on started_at IS NOT NULL.
                // language="PostgreSQL"
                """
                    CREATE INDEX
                    ON $cronTicks (name, started_at)
                    WHERE started_at IS NOT NULL
                """
            },
        ),
    )

    // Independent of MessageQueueSetup's own lock: a MessageQueueSink and a CronSink initialising
    // concurrently on the same prefix must not contend on each other's lock, since neither
    // touches the other's table.
    context(tables: CronTables)
    private fun advisoryLockKey(): Long = "kcpt:${tables.crons}".hashCode().toLong()

    context(tables: CronTables)
    internal suspend fun initializeCronSchema(source: Source): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            // Shared and idempotent — whichever sink (or process) gets here first creates
            // it; everyone else's call is a no-op. Neither sink owns it exclusively.
            Property.initializeTable(connection)

            connection.transaction {
                // Same reasoning as MessageQueueSetup.initializeMQSchema's own lock: reading the
                // version and acting on it is a check-then-act across sessions, so it needs to be
                // exclusive database-wide, not merely per-sink.
                prepareStatement("SELECT pg_advisory_xact_lock(?)").use { statement ->
                    statement.setLong(1, advisoryLockKey())
                    statement.execute()
                }

                val targetVersion = cronMigrations.size
                var currentVersion = getProperty(cronSchemaVersion) ?: 0

                // Same rolling-deployment rationale as MessageQueueSetup.initializeMQSchema's own check.
                check(currentVersion <= targetVersion) {
                    "${tables.crons} is at schema version $currentVersion, but this version of " +
                        "KCPT only knows up to $targetVersion: these tables have been migrated " +
                        "by a newer release and cannot be used by this one"
                }

                while (currentVersion < targetVersion) {
                    cronMigrations[currentVersion].forEach { sql ->
                        prepareStatement(sql(tables).trimIndent()).use { it.execute() }
                    }
                    ++currentVersion
                    setProperty(cronSchemaVersion, currentVersion)
                }
            }
        }
    }
}
