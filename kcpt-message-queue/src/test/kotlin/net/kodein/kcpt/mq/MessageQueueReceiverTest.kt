package net.kodein.kcpt.mq

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import kotlinx.coroutines.withContext
import net.kodein.kcpt.AbstractPgsqlTest
import net.kodein.kcpt.asListenFailingSource
import org.junit.Test
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

class MessageQueueReceiverTest : AbstractPgsqlTest() {

    // ReceiverImpl.close() used to call flowDef.getCompleted() unconditionally once the deferred
    // was completed — but getCompleted() rethrows if the deferred failed rather than produced a
    // ConnectionFlow, which is exactly what happens when opening the listen connection fails.
    //
    // A SupervisorJob isolates the receiver from this test's own coroutine, so its failure cancels
    // neither — nothing here has caught or observed it by the time close() runs, the position a
    // caller who never gets as far as receiveNext() would be in (a Receiver opened up front and
    // closed again on a path that never used it, or one closed during shutdown before its first
    // receiveNext() completed). Before the fix, close() rethrows the same failure as a brand new,
    // uncaught exception out of what is meant to be a plain "release this resource" call.
    @Test
    fun closeDoesNotThrowWhenOpeningTheListenConnectionFailed() = runTest(timeout = 5.seconds) {
        withContext(Dispatchers.Default) {
            val prefix = uniquePrefix()
            val mq = MessageQueueSink(source = dataSource.asListenFailingSource(), tablesPrefix = prefix)
                .messageQueue(uniqueQueueName(), User.serializer())

            val receiverScope = CoroutineScope(Dispatchers.Default + SupervisorJob())
            val receiver = mq.receiver(receiverScope)
            delay(300.milliseconds) // let the eagerly-started listen attempt fail

            receiver.close() // must not throw
            receiverScope.cancel()
        }
    }
}
