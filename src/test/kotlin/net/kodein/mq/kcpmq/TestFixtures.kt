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

    data class RecordedNotification(val channel: String, val payload: String, val cause: Throwable)

    val successes = CopyOnWriteArrayList<Recorded<T>>()
    val failures = CopyOnWriteArrayList<Recorded<T>>()
    val undecodableNotifications = CopyOnWriteArrayList<RecordedNotification>()

    override fun onSuccess(id: Uuid, payload: T, notes: JsonElement) {
        successes += Recorded(id, payload, notes)
    }

    override fun onFailure(id: Uuid, payload: T, notes: JsonElement) {
        failures += Recorded(id, payload, notes)
    }

    override fun onUndecodableNotification(channel: String, payload: String, cause: Throwable) {
        undecodableNotifications += RecordedNotification(channel, payload, cause)
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
