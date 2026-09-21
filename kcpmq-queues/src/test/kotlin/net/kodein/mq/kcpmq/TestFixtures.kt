@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import com.zaxxer.hikari.HikariDataSource
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.JsonElement
import java.time.OffsetDateTime
import java.util.concurrent.CopyOnWriteArrayList
import kotlin.time.Instant
import kotlin.uuid.Uuid
import kotlin.uuid.toJavaUuid


@Serializable
data class User(val id: Int, val name: String)

// A superset of User's wire shape, used to prove (or disprove) lenient decoding of unknown fields.
@Serializable
data class UserV2(val id: Int, val name: String, val extra: String = "x")

class RecordingObserver<T> : Message.Observer<T> {
    data class Recorded<T>(val id: Uuid, val payload: T, val notes: JsonElement)

    val successes = CopyOnWriteArrayList<Recorded<T>>()
    val failures = CopyOnWriteArrayList<Recorded<T>>()

    override fun onSuccess(id: Uuid, payload: T, notes: JsonElement) {
        successes += Recorded(id, payload, notes)
    }

    override fun onFailure(id: Uuid, payload: T, notes: JsonElement) {
        failures += Recorded(id, payload, notes)
    }
}

// A badly-behaved observer: throws from whichever callbacks it is told to, and records every call
// it was given so a test can tell "never invoked" from "invoked and threw".
class ThrowingObserver<T>(
    private val onSuccess: Boolean = false,
    private val onFailure: Boolean = false,
    private val onUndecodable: Boolean = false,
) : Message.Observer<T> {
    val calls = CopyOnWriteArrayList<String>()

    override fun onSuccess(id: Uuid, payload: T, notes: JsonElement) {
        calls += "success"
        if (onSuccess) throw RuntimeException("observer boom: onSuccess")
    }

    override fun onFailure(id: Uuid, payload: T, notes: JsonElement) {
        calls += "failure"
        if (onFailure) throw RuntimeException("observer boom: onFailure")
    }

    override fun onUndecodable(id: Uuid, payloadJson: String, notes: JsonElement) {
        calls += "undecodable"
        if (onUndecodable) throw RuntimeException("observer boom: onUndecodable")
    }
}

// Records what a sink reports about notifications it could not decode.
class RecordingUndecodableNotifications {
    data class Recorded(val channel: String, val payload: String, val cause: Throwable)

    val recorded = CopyOnWriteArrayList<Recorded>()

    val handler: (String, String, Throwable) -> Unit = { channel, payload, cause ->
        recorded += Recorded(channel, payload, cause)
    }
}

// Speaks to a channel the way any other database session could. NOTIFY is open to any role that can
// connect and the channel name is derivable from the table prefix and queue name, so this is what a
// foreign producer — or one on a wire format this consumer does not know — looks like from here.
fun HikariDataSource.rawNotify(channel: String, payload: String) =
    connection.use { connection ->
        connection.prepareStatement("SELECT pg_notify(?, ?)").use { statement ->
            statement.setString(1, channel)
            statement.setString(2, payload)
            statement.execute()
        }
    }

fun HikariDataSource.countMessages(prefix: String, queue: String): Int =
    connection.use { connection ->
        connection.prepareStatement("SELECT count(*) FROM ${prefix}messages WHERE queue = ?").use { statement ->
            statement.setString(1, queue)
            statement.executeQuery().use { resultSet -> resultSet.next(); resultSet.getInt(1) }
        }
    }

fun HikariDataSource.readNotesJson(prefix: String, id: Uuid): String =
    connection.use { connection ->
        connection.prepareStatement("SELECT notes FROM ${prefix}messages WHERE id = ?").use { statement ->
            statement.setObject(1, id.toJavaUuid())
            statement.executeQuery().use { resultSet -> resultSet.next(); resultSet.getString(1) }
        }
    }

fun HikariDataSource.startedAtOf(prefix: String, id: Uuid): OffsetDateTime? =
    connection.use { connection ->
        connection.prepareStatement("SELECT started_at FROM ${prefix}messages WHERE id = ?").use { statement ->
            statement.setObject(1, id.toJavaUuid())
            statement.executeQuery().use { resultSet ->
                resultSet.next()
                resultSet.getObject(1, OffsetDateTime::class.java)
            }
        }
    }

// Backdating a lease says "this message was picked up at T" without having to wait until T is in
// the past, so a test about the lease window does not have to bet on how long a delay really took.
fun HikariDataSource.setStartedAt(prefix: String, id: Uuid, startedAt: Instant) =
    connection.use { connection ->
        connection.prepareStatement("UPDATE ${prefix}messages SET started_at = ? WHERE id = ?").use { statement ->
            statement.setInstant(1, startedAt)
            statement.setObject(2, id.toJavaUuid())
            statement.executeUpdate()
        }
    }

fun HikariDataSource.dueAtOf(prefix: String, id: Uuid): OffsetDateTime? =
    connection.use { connection ->
        connection.prepareStatement("SELECT due_at FROM ${prefix}messages WHERE id = ?").use { statement ->
            statement.setObject(1, id.toJavaUuid())
            statement.executeQuery().use { resultSet ->
                resultSet.next()
                resultSet.getObject(1, OffsetDateTime::class.java)
            }
        }
    }

fun HikariDataSource.rawInsertMessage(
    prefix: String,
    queue: String,
    id: Uuid,
    dueAt: Instant,
    payloadJson: String,
    notesJson: String = "[]",
) = connection.use { connection ->
    connection.prepareStatement(
        "INSERT INTO ${prefix}messages (queue, id, due_at, payload, notes) VALUES (?, ?, ?, ?::json, ?::json)"
    ).use { statement ->
        statement.setString(1, queue)
        statement.setObject(2, id.toJavaUuid())
        statement.setInstant(3, dueAt)
        statement.setString(4, payloadJson)
        statement.setString(5, notesJson)
        statement.executeUpdate()
    }
}

// One INSERT … SELECT rather than n round trips: these helpers exist to get a table big enough for
// the planner to have an opinion, not to exercise the write path.
fun HikariDataSource.bulkInsertMessages(prefix: String, queue: String, count: Int) =
    connection.use { connection ->
        connection.prepareStatement(
            """
                INSERT INTO ${prefix}messages (queue, id, due_at, payload, notes)
                SELECT ?, gen_random_uuid(), now() - (g || ' seconds')::interval,
                       '{"id":1,"name":"bulk"}'::json, '[]'::json
                FROM generate_series(1, ?) g
            """.trimIndent()
        ).use { statement ->
            statement.setString(1, queue)
            statement.setInt(2, count)
            statement.executeUpdate()
        }
    }

// Without statistics the planner has no idea how big the table is, and a sequential scan is a
// defensible choice whatever indexes exist.
fun HikariDataSource.analyze(prefix: String) =
    connection.use { connection ->
        connection.createStatement().use { it.execute("ANALYZE ${prefix}messages") }
    }

fun HikariDataSource.explain(sql: String): List<String> =
    connection.use { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery("EXPLAIN $sql").use { resultSet ->
                generateSequence { if (resultSet.next()) resultSet.getString(1) else null }.toList()
            }
        }
    }
