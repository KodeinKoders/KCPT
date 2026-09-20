@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import java.sql.Connection
import kotlin.use

internal class Property<T : Any>(val key: String, val encoder: Encoder<T>) {
    sealed interface Encoder<T : Any> {
        fun encode(value: T): String
        fun decode(value: String): T?
        object StringEncoder : Encoder<String> {
            override fun encode(value: String): String = value
            override fun decode(value: String): String = value
        }
        object IntEncoder : Encoder<Int> {
            override fun encode(value: Int): String = value.toString()
            override fun decode(value: String): Int? = value.toIntOrNull()
        }
    }
    companion object {
        val SchemaVersion = Property("schema_version", Encoder.IntEncoder)
    }
}

context(tables: MQTables)
internal suspend fun <T : Any> Connection.getProperty(property: Property<T>): T? = withContext(Dispatchers.IO) {
    prepareStatement("SELECT value FROM ${tables.properties} WHERE key = ?").use { statement ->
        statement.setString(1, property.key)
        statement.executeQuery().use { resultSet ->
            if (resultSet.next()) resultSet.getString("value") else null
        }
    }?.let {
        property.encoder.decode(it)
    }
}

context(tables: MQTables)
internal suspend fun <T : Any> Connection.setProperty(property: Property<T>, value: T) = withContext(Dispatchers.IO) {
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
