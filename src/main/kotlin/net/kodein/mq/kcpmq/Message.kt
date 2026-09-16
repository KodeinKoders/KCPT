@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlin.time.Clock
import kotlin.time.Instant
import kotlin.uuid.Uuid
import kotlin.uuid.toJavaUuid


public interface Message<T> {
    public val id: Uuid
    public val payload: T
    public val notes: List<JsonElement>
    public suspend fun successDelete(notes: JsonElement = JsonNull)
    public suspend fun failureDelete(notes: JsonElement = JsonNull)
    public suspend fun failureRequeue(due: Instant? = null, notes: JsonElement = JsonNull)
    public suspend fun extend()

    public interface Observer<in T> {
        public fun onSuccess(id: Uuid, payload: T, notes: JsonElement)
        public fun onFailure(id: Uuid, payload: T, notes: JsonElement)
    }
}

internal class MessageImpl<T>(
    override val id: Uuid,
    override val payload: T,
    override val notes: List<JsonElement>,
    private val maxTries: Int,
    private val source: Source,
    private val tables: MQTables,
    private val observers: List<Message.Observer<T>> = emptyList(),
) : Message<T> {

    override suspend fun successDelete(notes: JsonElement) {
        source().use { connection ->
            connection.prepareStatement("DELETE FROM ${tables.messages} WHERE id = ?").use { statement ->
                statement.setObject(1, id.toJavaUuid())
                statement.executeUpdate()
            }
        }
        observers.forEach { it.onSuccess(id, payload, notes) }
    }

    override suspend fun failureRequeue(due: Instant?, notes: JsonElement) {
        val updatedNotes = JsonArray(this.notes + notes)
        if (updatedNotes.size >= maxTries) {
            failureDelete(notes)
            return
        }

        source().use { connection ->
            connection.prepareStatement("UPDATE ${tables.messages} SET due_at = ?, notes = ?::json, started_at = NULL WHERE id = ?").use { statement ->
                statement.setInstant(1, due ?: Clock.System.now())
                statement.setString(2, mqJson.encodeToString(JsonArray.serializer(), updatedNotes))
                statement.setObject(3, id.toJavaUuid())
                statement.executeUpdate()
            }
        }
    }

    override suspend fun failureDelete(notes: JsonElement) {
        source().use { connection ->
            connection.prepareStatement("DELETE FROM ${tables.messages} WHERE id = ?").use { statement ->
                statement.setObject(1, id.toJavaUuid())
                statement.executeUpdate()
            }
        }
        observers.forEach { it.onFailure(id, payload, notes) }
    }

    override suspend fun extend() {
        source().use { connection ->
            connection.prepareStatement("UPDATE ${tables.messages} SET started_at = ? WHERE id = ?").use { statement ->
                statement.setInstant(1, Clock.System.now())
                statement.setObject(2, id.toJavaUuid())
                statement.executeUpdate()
            }
        }
    }
}
