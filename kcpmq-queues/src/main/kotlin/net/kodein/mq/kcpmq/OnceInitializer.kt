package net.kodein.mq.kcpmq

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlin.concurrent.atomics.AtomicReference

// Runs `block` exactly once, sharing the result across however many callers invoke `initialize()`
// concurrently — schema-agnostic, so both MessageQueueSink and CronSink can wrap their own setup
// call in one of these without duplicating the compare-and-exchange logic below.
internal class OnceInitializer(
    initializationScope: CoroutineScope?,
    private val block: suspend () -> Unit,
) {
    // Lazy initialisation is shared by every caller, so it must not run in whichever caller's
    // scope happened to get there first: cancelling that caller would cancel it and leave every
    // other waiter with a cancellation that was never theirs. A one-shot init holds nothing once
    // it finishes, so this scope is never torn down.
    private val initScope: CoroutineScope = CoroutineScope(Dispatchers.IO + SupervisorJob())

    private val initRef: AtomicReference<Deferred<Unit>?> = AtomicReference(null)

    init {
        if (initializationScope != null) {
            // Eager initialisation is owned by the scope the caller handed over, so it stays a
            // child of it: they decide its lifetime, and awaiting that scope awaits this.
            // No need to use [initialize] since there cannot be construction race conditions.
            initRef.store(initializationScope.async { block() })
        }
    }

    suspend fun initialize() {
        initRef.load()?.let { return awaitInit(it) }

        val candidate = initScope.async(start = CoroutineStart.LAZY) { block() }
        val winner = initRef.compareAndExchange(null, candidate)
        if (winner != null) {
            candidate.cancel()
            return awaitInit(winner)
        }
        candidate.start()
        return awaitInit(candidate)
    }

    // Job.join() reports nothing: it returns just as happily for a job that failed as for one that
    // succeeded, which is how a failed initialisation used to be remembered as a success. Deferred
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
}
