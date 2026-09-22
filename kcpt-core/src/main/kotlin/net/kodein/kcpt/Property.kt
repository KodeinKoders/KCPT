@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.kcpt

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import kotlin.use

@InternalKcptApi
public class Property<T : Any>(public val key: String, public val encoder: Encoder<T>) {
    public sealed interface Encoder<T : Any> {
        public fun encode(value: T): String
        public fun decode(value: String): T?
        public object StringEncoder : Encoder<String> {
            override fun encode(value: String): String = value
            override fun decode(value: String): String = value
        }
        public object IntEncoder : Encoder<Int> {
            override fun encode(value: Int): String = value.toString()
            override fun decode(value: String): Int? = value.toIntOrNull()
        }
    }
    public companion object {
        // Called before a feature module's own schema-migration transaction (under its own
        // table-keyed lock), in a separate transaction of its own — a different lock downstream,
        // so without a lock here, a MessageQueueSink and a CronSink initialising concurrently on
        // the same prefix could both reach CREATE TABLE IF NOT EXISTS at once. That statement is
        // not atomic against a concurrent creator, and the loser would fail with a duplicate
        // pg_type catalog entry rather than seeing the table the winner just made. Keyed on
        // tables.properties, independent of either sink's own lock, since it protects a resource
        // neither sink owns exclusively.
        @InternalKcptApi
        context(tables: HasPropertiesTable)
        public suspend fun initializeTable(connection: Connection): Unit = withContext(Dispatchers.IO) {
            connection.transaction {
                connection.prepareStatement("SELECT pg_advisory_xact_lock(?)").use { statement ->
                    statement.setLong(1, "kcpt:${tables.properties}".hashCode().toLong())
                    statement.execute()
                }
                connection.prepareStatement("CREATE TABLE IF NOT EXISTS ${tables.properties} (key VARCHAR(32) PRIMARY KEY, value TEXT)").use {
                    it.execute()
                }
            }
        }

    }
}

@InternalKcptApi
context(tables: HasPropertiesTable)
public suspend fun <T : Any> Connection.getProperty(property: Property<T>): T? = withContext(Dispatchers.IO) {
    val raw = prepareStatement("SELECT value FROM ${tables.properties} WHERE key = ?").use { statement ->
        statement.setString(1, property.key)
        statement.executeQuery().use { resultSet ->
            if (resultSet.next()) resultSet.getString("value") else null
        }
    } ?: return@withContext null

    // Absent (above) and present-but-unreadable are different problems: the first is an ordinary
    // fresh database, the second is corruption or a hand-edit, and a schema-migration caller
    // treats a null the same as "not yet migrated" — collapsing the two would let corrupted state
    // re-run the migration from scratch against tables that already exist, surfacing as "CREATE
    // TABLE ... already exists" rather than as the data problem it actually is.
    checkNotNull(property.encoder.decode(raw)) {
        "Property '${property.key}' in ${tables.properties} has a value that could not be decoded: '$raw'"
    }
}

@InternalKcptApi
context(tables: HasPropertiesTable)
public suspend fun <T : Any> Connection.setProperty(property: Property<T>, value: T): Unit = withContext(Dispatchers.IO) {
    val encodedValue = property.encoder.encode(value)
    // EXCLUDED.value is the value this statement tried to insert, so the update cannot disagree with
    // the insert. Binding it a second time said the same thing in a way that two edits could pull
    // apart.
    prepareStatement("INSERT INTO ${tables.properties} (key, value) VALUES (?, ?) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value")
        .use {
            it.setString(1, property.key)
            it.setString(2, encodedValue)
            it.execute()
        }
}
