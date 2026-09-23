package net.kodein.kcpt.mq

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.sync.Semaphore
import kotlinx.serialization.json.JsonElement
import kotlin.time.Duration


@OptIn(DelicateCoroutinesApi::class)
public suspend fun <T> MessageQueue<T>.workerPool(
    numberOfThreads: Int = Runtime.getRuntime().availableProcessors(),
    numberOfWorkers: Int = numberOfThreads * 4,
    timeout: Duration = config.processingTimeout,
    onRequeueFailure: (message: Message<T>, failure: MessageProcessingFailure, cause: Throwable) -> Unit = { _, _, _ -> },
    process: suspend (Message<T>) -> JsonElement?
): Nothing {
    newFixedThreadPoolContext(numberOfThreads, "KCPT-WorkerPool").use { dispatcher ->
        val semaphore = Semaphore(numberOfWorkers)
        coroutineScope {
            receiver(this).use { receiver ->
                while (true) {
                    semaphore.acquire()
                    val message =
                        try { receiver.receiveNext() }
                        catch (e: Throwable) {
                            semaphore.release()
                            throw e
                        }
                    launch(dispatcher) {
                        try {
                            message.process(
                                timeout = timeout,
                                onRequeueFailure = onRequeueFailure,
                                process = process
                            )
                        } finally {
                            semaphore.release()
                        }
                    }
                }
            }
        }
    }
}
