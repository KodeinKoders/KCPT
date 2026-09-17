@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.serialization.json.JsonArray
import kotlinx.serialization.json.JsonElement
import kotlinx.serialization.json.JsonNull
import kotlinx.serialization.json.JsonObject
import kotlinx.serialization.json.JsonPrimitive
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Clock
import kotlin.time.Duration
import kotlin.time.Instant
import kotlin.uuid.Uuid
import kotlin.uuid.toJavaUuid


public interface Message<T> {
    public val id: Uuid
    public val payload: T
    public val receivedAt: Instant
    public val expiresAt: Instant
    public val notes: List<JsonElement>
    public suspend fun successDelete(notes: JsonElement = JsonNull)
    public suspend fun failureDelete(notes: JsonElement = JsonNull)
    public suspend fun failureRequeue(due: Instant? = null, notes: JsonElement = JsonNull)
    public suspend fun extend()

    public interface Observer<in T> {
        public fun onSuccess(id: Uuid, payload: T, notes: JsonElement)
        public fun onFailure(id: Uuid, payload: T, notes: JsonElement)

        // Dropped after its payload could not be decoded, so there is no T to report — the raw
        // JSON is given instead, since it is the only remaining record of the message.
        public fun onUndecodable(id: Uuid, payloadJson: String, notes: JsonElement) {}

        // Not a message at all: a wake-up notification on the queue's channel whose payload could
        // not be read, so there is no id and no T. It is dropped rather than thrown, because a
        // notification is only ever a hint that work may be available — but it is reported rather
        // than swallowed, because the usual cause is a producer writing a format this consumer
        // cannot read, and nothing else in the process would ever say so.
        public fun onUndecodableNotification(channel: String, payload: String, cause: Throwable) {}
    }
}

internal class MessageImpl<T>(
    override val id: Uuid,
    override val payload: T,
    override val receivedAt: Instant,
    private val mqConfig: MessageQueue.Config,
    override val notes: List<JsonElement>,
    private val source: Source,
    private val tables: MQTables,
    private val observers: List<Message.Observer<T>> = emptyList(),
) : Message<T> {

    override var expiresAt: Instant = receivedAt + mqConfig.processingTimeout
        private set

    // Source only moves connection *acquisition* to Dispatchers.IO, so without this the blocking
    // statement would run on whichever dispatcher the caller is on: a core-count-limited pool it
    // can starve, a single thread it can freeze, or — via process()'s Unconfined keepalive — the
    // shared timer thread, where it would hold up every delay() in the process.
    override suspend fun successDelete(notes: JsonElement): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.prepareStatement("DELETE FROM ${tables.messages} WHERE id = ?").use { statement ->
                statement.setObject(1, id.toJavaUuid())
                statement.executeUpdate()
            }
        }
        observers.forEach { it.onSuccess(id, payload, notes) }
    }

    override suspend fun failureRequeue(due: Instant?, notes: JsonElement): Unit = withContext(Dispatchers.IO) {
        val updatedNotes = JsonArray(this@MessageImpl.notes + notes)
        if (updatedNotes.size >= mqConfig.maxTries) {
            failureDelete(notes)
            return@withContext
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

    override suspend fun failureDelete(notes: JsonElement): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            connection.prepareStatement("DELETE FROM ${tables.messages} WHERE id = ?").use { statement ->
                statement.setObject(1, id.toJavaUuid())
                statement.executeUpdate()
            }
        }
        observers.forEach { it.onFailure(id, payload, notes) }
    }

    override suspend fun extend(): Unit = withContext(Dispatchers.IO) {
        source().use { connection ->
            val now = Clock.System.now()
            connection.prepareStatement("UPDATE ${tables.messages} SET started_at = ? WHERE id = ?").use { statement ->
                statement.setInstant(1, now)
                statement.setObject(2, id.toJavaUuid())
                statement.executeUpdate()
            }
            expiresAt = now + mqConfig.processingTimeout
        }
    }
}

public sealed class MessageProcessingFailure {
    public abstract fun defaultNotes(): Map<String, JsonElement>

    public data class Exception(public val exception: Throwable) : MessageProcessingFailure() {
        override fun defaultNotes(): Map<String, JsonElement> =
            mapOf(
                "cause" to JsonPrimitive("exception"),
                "exceptionMessage" to JsonPrimitive(exception.message),
                "exceptionType" to JsonPrimitive(exception::class.qualifiedName),
                "exceptionStackTrace" to JsonPrimitive(exception.stackTraceToString()),
            )
    }
    public data class Timeout(public val timeout: Duration) : MessageProcessingFailure() {
        override fun defaultNotes(): Map<String, JsonElement> =
            mapOf(
                "cause" to JsonPrimitive("timeout"),
                "timeoutMs" to JsonPrimitive(timeout.inWholeMilliseconds),
            )
    }
    public data class Cancellation(val cancelledFromWork: Boolean) : MessageProcessingFailure() {
        override fun defaultNotes(): Map<String, JsonElement> =
            mapOf(
                "cause" to JsonPrimitive("cancellation"),
                "from" to JsonPrimitive(if (cancelledFromWork) "work" else "parent"),
            )
    }
}

public suspend fun <T> Message<T>.process(
    timeout: Duration = this.expiresAt - Clock.System.now(),
    keepalive: Duration? = null,
    onFailure: (failure: MessageProcessingFailure) -> JsonElement = { JsonObject(it.defaultNotes()) },
    process: suspend (Message<T>) -> JsonElement?
) {
    require(keepalive == null || keepalive < timeout) { "Keepalive must be less than message processing timeout" }
    coroutineScope {
        try {
            withTimeout(timeout) {
                val keepAliveJob =
                    if (keepalive != null) {
                        launch(Dispatchers.Unconfined) {
                            while (true) {
                                delay(keepalive)
                                extend()
                            }
                        }
                    } else null
                val notes = process(this@process)
                successDelete(notes ?: JsonNull)
                keepAliveJob?.cancelAndJoin()
                true
            }
        } catch (_: TimeoutCancellationException) {
            withContext(NonCancellable) {
                failureRequeue(notes = onFailure(MessageProcessingFailure.Timeout(timeout)))
            }
        } catch (_: CancellationException) {
            // Read before entering NonCancellable: NonCancellable is an always-active Job that
            // would shadow this scope's Job and make isActive always report true.
            val cancelledFromWork = isActive
            withContext(NonCancellable) {
                failureRequeue(notes = onFailure(MessageProcessingFailure.Cancellation(cancelledFromWork)))
            }
            ensureActive()
        } catch (t: Throwable) {
            withContext(NonCancellable) {
                failureRequeue(notes = onFailure(MessageProcessingFailure.Exception(t)))
            }
        }
    }
}
