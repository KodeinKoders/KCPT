@file:Suppress("SqlSourceToSinkFlow")
@file:OptIn(InternalKcptApi::class)

package net.kodein.kcpt.cron

import com.zaxxer.hikari.HikariDataSource
import net.kodein.kcpt.InternalKcptApi
import net.kodein.kcpt.setInstant
import java.time.OffsetDateTime
import kotlin.time.Instant

// Seeds (or overwrites) a cron's cursor row directly, bypassing CronSink.cron()'s registration
// entirely — the way a test backdates a scenario (e.g. simulating downtime) rather than waiting
// for real time to pass.
fun HikariDataSource.rawInsertCronCursor(prefix: String, name: String, nextSlotAt: Instant) =
    connection.use { connection ->
        connection.prepareStatement(
            """
                INSERT INTO ${prefix}crons (name, next_slot_at) VALUES (?, ?)
                ON CONFLICT (name) DO UPDATE SET next_slot_at = EXCLUDED.next_slot_at
            """.trimIndent()
        ).use { statement ->
            statement.setString(1, name)
            statement.setInstant(2, nextSlotAt)
            statement.executeUpdate()
        }
    }

fun HikariDataSource.cronNextSlotAtOf(prefix: String, name: String): OffsetDateTime? =
    connection.use { connection ->
        connection.prepareStatement("SELECT next_slot_at FROM ${prefix}crons WHERE name = ?").use { statement ->
            statement.setString(1, name)
            statement.executeQuery().use { resultSet ->
                if (!resultSet.next()) null else resultSet.getObject(1, OffsetDateTime::class.java)
            }
        }
    }

// Materializes a tick row directly, bypassing the normal materialize step — the way a test sets
// up a lease/retry/reclaim scenario without needing a cron to actually reach that state on its
// own.
fun HikariDataSource.rawInsertCronTick(
    prefix: String,
    name: String,
    fromAt: Instant,
    toAt: Instant,
    startedAt: Instant? = null,
    notesJson: String = "[]",
) = connection.use { connection ->
    connection.prepareStatement(
        """
            INSERT INTO ${prefix}cron_ticks (name, from_at, to_at, started_at, notes)
            VALUES (?, ?, ?, ?, ?::json)
            ON CONFLICT (name, from_at) DO UPDATE SET
                to_at = EXCLUDED.to_at,
                started_at = EXCLUDED.started_at,
                notes = EXCLUDED.notes
        """.trimIndent()
    ).use { statement ->
        statement.setString(1, name)
        statement.setInstant(2, fromAt)
        statement.setInstant(3, toAt)
        if (startedAt != null) statement.setInstant(4, startedAt) else statement.setNull(4, java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
        statement.setString(5, notesJson)
        statement.executeUpdate()
    }
}

fun HikariDataSource.cronTickStartedAtOf(prefix: String, name: String, fromAt: Instant): OffsetDateTime? =
    connection.use { connection ->
        connection.prepareStatement("SELECT started_at FROM ${prefix}cron_ticks WHERE name = ? AND from_at = ?").use { statement ->
            statement.setString(1, name)
            statement.setInstant(2, fromAt)
            statement.executeQuery().use { resultSet ->
                resultSet.next()
                resultSet.getObject(1, OffsetDateTime::class.java)
            }
        }
    }

fun HikariDataSource.cronTickNotesJson(prefix: String, name: String, fromAt: Instant): String =
    connection.use { connection ->
        connection.prepareStatement("SELECT notes FROM ${prefix}cron_ticks WHERE name = ? AND from_at = ?").use { statement ->
            statement.setString(1, name)
            statement.setInstant(2, fromAt)
            statement.executeQuery().use { resultSet -> resultSet.next(); resultSet.getString(1) }
        }
    }
