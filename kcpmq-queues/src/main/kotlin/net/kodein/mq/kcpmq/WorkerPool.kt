package net.kodein.mq.kcpmq

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
    keepalive: Duration? = if (timeout > config.processingTimeout) config.processingTimeout * .6 else null,
    process: suspend (Message<T>) -> JsonElement?
): Nothing {
    require(keepalive == null || keepalive < config.processingTimeout) { "Worker pool keepalive must be less than message queue processing timeout" }
    require(keepalive == null || keepalive < timeout) { "Worker pool keepalive must be less than Worker pool processing timeout" }
    newFixedThreadPoolContext(numberOfThreads, "KCPMQ-WorkerPool").use { dispatcher ->
        val semaphore = Semaphore(numberOfWorkers)
        coroutineScope {
            while (true) {
                semaphore.acquire()
                val message =
                    try { receive() }
                    catch (e: Throwable) {
                        semaphore.release()
                        throw e
                    }
                launch(dispatcher) {
                    try {
                        message.process(
                            timeout = timeout,
                            keepalive = keepalive,
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
