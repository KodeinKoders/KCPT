@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.kcpt

import com.zaxxer.hikari.HikariDataSource
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.SQLException
import java.util.concurrent.atomic.AtomicInteger


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

// Writes a properties row directly, bypassing setProperty's encoder entirely — the way a hand-edit
// or storage corruption would leave a value the encoder was never asked to produce.
fun HikariDataSource.rawSetProperty(prefix: String, key: String, value: String) =
    connection.use { connection ->
        connection.prepareStatement(
            "INSERT INTO ${prefix}properties (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value"
        ).use { statement ->
            statement.setString(1, key)
            statement.setString(2, value)
            statement.executeUpdate()
        }
    }

fun HikariDataSource.explain(sql: String): List<String> =
    connection.use { connection ->
        connection.createStatement().use { statement ->
            statement.executeQuery("EXPLAIN $sql").use { resultSet ->
                generateSequence { if (resultSet.next()) resultSet.getString(1) else null }.toList()
            }
        }
    }

// Wraps a real connection so every LISTEN it executes is counted — openListenConnection issues
// exactly one per connection it opens, so counting it is what proves whether a listening
// connection is being reused across many receiveNext() calls or reopened per one.
fun HikariDataSource.countingListenSource(listens: AtomicInteger): Source {
    val real = asSource()
    return {
        val connection = real()
        Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java),
        ) { _, method, args ->
            if (method.name == "prepareStatement" && (args?.getOrNull(0) as? String)?.startsWith("LISTEN ") == true) {
                listens.incrementAndGet()
            }
            if (args == null) method.invoke(connection) else method.invoke(connection, *args)
        } as Connection
    }
}

// Wraps a real connection so LISTEN — the statement openListenConnection issues right after
// acquiring a connection — throws instead of executing, the shape of a dropped connection or an
// exhausted pool during that handshake. Everything else (post, tryReceive, schema setup) goes
// through untouched, since none of it ever issues LISTEN.
fun HikariDataSource.asListenFailingSource(): Source {
    val real = asSource()
    return {
        val connection = real()
        Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java),
        ) { _, method, args ->
            if (method.name == "prepareStatement" && (args?.getOrNull(0) as? String)?.startsWith("LISTEN ") == true) {
                throw SQLException("connection blip")
            }
            if (args == null) method.invoke(connection) else method.invoke(connection, *args)
        } as Connection
    }
}
