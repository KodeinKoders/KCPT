package net.kodein.mq.kcpmq

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.serialization.KSerializer
import kotlin.concurrent.atomics.AtomicReference


public interface MessageQueueSink {
    public suspend fun initialize()

    public suspend fun <T> messageQueue(
        name: String,
        serializer: KSerializer<T>,
        config: MessageQueue.Config = MessageQueue.Config(),
        messageObservers: List<Message.Observer<T>> = emptyList(),
    ): MessageQueue<T>
}

public fun MessageQueueSink(
    source: Source,
    tablesPrefix: String = "public.mq_",
    initializationScope: CoroutineScope? = null,
    messageObservers: List<Message.Observer<Any?>> = emptyList(),
    // Told about a notification on a queue's channel whose payload could not be read. Such a
    // notification is dropped — it is only ever a hint that work may be available — so without this
    // nothing in the process would report that a producer is writing a format these consumers
    // cannot read. The channel name embeds the queue name, so one handler can tell queues apart.
    onUndecodableNotification: (channel: String, payload: String, cause: Throwable) -> Unit =
        { _, _, _ -> },
): MessageQueueSink = MessageQueueSinkImpl(
    source = source,
    tablesPrefix = tablesPrefix,
    initializationScope = initializationScope,
    messageObservers = messageObservers,
    onUndecodableNotification = onUndecodableNotification,
)

internal class MessageQueueSinkImpl(
    private val source: Source,
    tablesPrefix: String,
    initializationScope: CoroutineScope?,
    private val messageObservers: List<Message.Observer<Any?>>,
    private val onUndecodableNotification:
        (channel: String, payload: String, cause: Throwable) -> Unit = { _, _, _ -> },
) : MessageQueueSink {
    private val tables = MQTables.named(
        properties = "${tablesPrefix}properties",
        messages = "${tablesPrefix}messages",
    )

    // Lazy initialisation is shared by every caller, so it must not run in whichever caller's
    // scope happened to get there first: cancelling that caller would cancel the migration and
    // leave every other waiter with a cancellation that was never theirs. A one-shot migration
    // holds nothing once it finishes, so this scope is never torn down.
    private val initScope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val initRef: AtomicReference<Deferred<Unit>?> = AtomicReference(null)

    init {
        if (initializationScope != null) {
            // Eager initialisation is owned by the scope the caller handed over, so it stays a
            // child of it: they decide its lifetime, and awaiting that scope awaits this.
            // No need to use [initialize] since there cannot be construction race conditions.
            initRef.store(
                initializationScope.async { context(tables) { Setup.initializeMQSchema(source) } }
            )
        }
    }

    override suspend fun initialize() {
        initRef.load()?.let { return awaitInit(it) }

        val candidate = initScope.async(start = CoroutineStart.LAZY) {
            context(tables) { Setup.initializeMQSchema(source) }
        }
        val winner = initRef.compareAndExchange(null, candidate)
        if (winner != null) {
            candidate.cancel()
            return awaitInit(winner)
        }
        candidate.start()
        return awaitInit(candidate)
    }

    // Job.join() reports nothing: it returns just as happily for a job that failed as for one that
    // succeeded, which is how a failed migration used to be remembered as a success. Deferred
    // propagates instead.
    private suspend fun awaitInit(deferred: Deferred<Unit>) {
        try {
            deferred.await()
        } catch (t: Throwable) {
            // Forget a failed initialisation so a later caller can retry — caching it would let one
            // transient outage poison the sink for the life of the process. But if the deferred is
            // still running, it was this caller that was cancelled, not the initialisation, and the
            // work others are waiting on must be left alone.
            if (!deferred.isActive) initRef.compareAndSet(deferred, null)
            throw t
        }
    }

    override suspend fun <T> messageQueue(
        name: String,
        serializer: KSerializer<T>,
        config: MessageQueue.Config,
        messageObservers: List<Message.Observer<T>>,
    ): MessageQueue<T> {
        initialize()
        return MessageQueueImpl(
            name = name,
            source = source,
            tables = tables,
            serializer = serializer,
            config = config,
            messageObservers = this.messageObservers + messageObservers,
            onUndecodableNotification = onUndecodableNotification,
        )
    }

}
