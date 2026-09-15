package net.kodein.mq.kcpmq

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.time.Duration


@OptIn(DelicateCoroutinesApi::class)
public suspend fun <T> MessageQueue<T>.workerPool(
    numberOfThreads: Int = Runtime.getRuntime().availableProcessors(),
    numberOfWorkers: Int = numberOfThreads * 4,
    workTimeout: Duration = config.messageTreatmentTimeout,
    workKeepalive: Duration = config.messageTreatmentTimeout * .8,
    work: suspend (Message<T>) -> Unit
): Nothing {
    require(workKeepalive < config.messageTreatmentTimeout) { "Work keepalive must be less than message treatment timeout" }
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
                        workMessage(
                            message = message,
                            workTimeout = workTimeout,
                            workKeepalive = if (workTimeout > config.messageTreatmentTimeout) workKeepalive else null,
                            work = work
                        )
                    } finally {
                        semaphore.release()
                    }
                }
            }
        }
    }
}

private suspend fun <T> workMessage(
    message: Message<T>,
    workTimeout: Duration,
    workKeepalive: Duration?,
    work: suspend (Message<T>) -> Unit
) {
    withTimeoutOrNull(workTimeout) {

    }
}