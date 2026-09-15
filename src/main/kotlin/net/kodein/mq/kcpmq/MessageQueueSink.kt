package net.kodein.mq.kcpmq

import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.serialization.KSerializer
import net.kodein.mq.kcpmq.Message
import kotlin.concurrent.atomics.AtomicReference
import kotlin.concurrent.atomics.ExperimentalAtomicApi


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
): MessageQueueSink = MessageQueueSinkImpl(
    source = source,
    tablesPrefix = tablesPrefix,
    initializationScope = initializationScope,
    messageObservers = messageObservers,
)

@OptIn(ExperimentalAtomicApi::class)
internal class MessageQueueSinkImpl(
    private val source: Source,
    tablesPrefix: String,
    initializationScope: CoroutineScope?,
    private val messageObservers: List<Message.Observer<Any?>>,
) : MessageQueueSink {
    private val tables = MQTables.named(
        properties = "${tablesPrefix}properties",
        messages = "${tablesPrefix}messages",
    )

    private val initJobRef: AtomicReference<Job?> = AtomicReference(null)

    init {
        require(tables.messages.isNotEmpty()) { "Table name cannot be empty" }
        if (initializationScope != null) {
            initJobRef.store(
                initializationScope.launch {
                    // No need to use [initialize] since there cannot be construction race conditions.
                    context(tables) {
                        Setup.initializeMQSchema(source)
                    }
                }
            )
        }
    }

    override suspend fun initialize() {
        val initJob = initJobRef.load()
        if (initJob != null) {
            initJob.join()
            return
        }
        coroutineScope {
            val first = CompletableDeferred<Job?>()
            val job = launch {
                val first = first.await()
                if (first != null) {
                    // Another job was first
                    first.join()
                    return@launch
                }
                // This job is first
                context(tables) {
                    Setup.initializeMQSchema(source)
                }
            }
            first.complete(initJobRef.compareAndExchange(null, job))
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
        )
    }

}
