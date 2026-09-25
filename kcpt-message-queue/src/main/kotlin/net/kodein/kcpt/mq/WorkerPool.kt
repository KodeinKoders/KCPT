package net.kodein.kcpt.mq

import kotlinx.coroutines.DelicateCoroutinesApi
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.newFixedThreadPoolContext
import kotlinx.coroutines.sync.Semaphore
import kotlinx.serialization.json.JsonElement
import kotlin.coroutines.cancellation.CancellationException
import kotlin.time.Duration


@OptIn(DelicateCoroutinesApi::class)
public suspend fun <T> MessageQueue<T>.workerPool(
    numberOfThreads: Int = Runtime.getRuntime().availableProcessors(),
    numberOfWorkers: Int = numberOfThreads * 4,
    timeout: Duration = config.processingTimeout,
    onRequeueFailure: (message: Message<T>, failure: MessageProcessingFailure, cause: Throwable) -> Unit = { _, _, _ -> },
    // process() itself only ever fails through failureRequeue/failureDelete's own write-back — every
    // other outcome is contained and reported through onFailure/onRequeueFailure before process()
    // returns. But process() can also throw before any of that machinery ever runs, most concretely
    // its own already-expired-lease check() (Message.kt) — a state the message was found in, not
    // something process() itself catches. Left unguarded here, that exception would escape this
    // launch and cancel workerPool's whole scope, taking every other in-flight message down with it,
    // reopening the exact class of bug onRequeueFailure exists to close on the write-back side.
    onUnhandledFailure: (message: Message<T>, cause: Throwable) -> Unit = { _, _ -> },
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
                        } catch (cause: CancellationException) {
                            // This coroutine is on the pool's own cancellable scope, not a
                            // NonCancellable boundary — swallowing this would break shutdown.
                            throw cause
                        } catch (cause: Throwable) {
                            // Guarded like onRequeueFailure's own call site in Message.kt: a throwing
                            // callback here must not reopen the exact hole this catch exists to close.
                            try { onUnhandledFailure(message, cause) } catch (_: Throwable) {}
                        } finally {
                            semaphore.release()
                        }
                    }
                }
            }
        }
    }
}
