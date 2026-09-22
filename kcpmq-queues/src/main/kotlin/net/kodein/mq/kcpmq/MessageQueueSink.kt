package net.kodein.mq.kcpmq

import kotlinx.coroutines.CoroutineScope
import kotlinx.serialization.KSerializer


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
        crons = "${tablesPrefix}crons",
        cronTicks = "${tablesPrefix}cron_ticks",
    )

    private val initializer = OnceInitializer(initializationScope) {
        context(tables) { MessageQueueSetup.initializeMQSchema(source) }
    }

    override suspend fun initialize() = initializer.initialize()

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
