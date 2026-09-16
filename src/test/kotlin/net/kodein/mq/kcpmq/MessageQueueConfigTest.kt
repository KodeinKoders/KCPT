package net.kodein.mq.kcpmq

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds

// Every one of these settings fails silently rather than loudly when out of range — a black hole,
// a hot spin, leases that expire on arrival — so they are rejected at construction instead.
class MessageQueueConfigTest {

    @Test
    fun theDefaultsAreValid() {
        MessageQueue.Config()
    }

    @Test
    fun maxTriesBelowOneIsRejected() {
        assertFailsWith<IllegalArgumentException> { MessageQueue.Config(maxTries = 0) }
        assertFailsWith<IllegalArgumentException> { MessageQueue.Config(maxTries = -1) }
        MessageQueue.Config(maxTries = 1) // one attempt then dead-letter is meaningful
    }

    @Test
    fun aNonPositiveSecurityCheckTimeoutIsRejected() {
        assertFailsWith<IllegalArgumentException> { MessageQueue.Config(securityCheckTimeout = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { MessageQueue.Config(securityCheckTimeout = (-1).seconds) }
    }

    @Test
    fun aNonPositiveProcessingTimeoutIsRejected() {
        assertFailsWith<IllegalArgumentException> { MessageQueue.Config(processingTimeout = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { MessageQueue.Config(processingTimeout = (-1).seconds) }
    }

    @Test
    fun aNegativeProcessingGraceIsRejected() {
        assertFailsWith<IllegalArgumentException> { MessageQueue.Config(processingGrace = (-1).milliseconds) }
        // Zero is a legitimate choice: it only gives up the margin, it does not invert it.
        MessageQueue.Config(processingGrace = Duration.ZERO)
    }

    @Test
    fun copyIsValidatedToo() {
        val valid = MessageQueue.Config()

        assertFailsWith<IllegalArgumentException> { valid.copy(maxTries = 0) }
        assertFailsWith<IllegalArgumentException> { valid.copy(processingTimeout = Duration.ZERO) }
        assertEquals(3, valid.copy(maxTries = 3).maxTries)
    }

    @Test
    fun theRejectionNamesTheOffendingValue() {
        val message = assertFailsWith<IllegalArgumentException> { MessageQueue.Config(maxTries = 0) }.message
        assertEquals(true, message?.contains("maxTries"), "got: $message")
        assertEquals(true, message?.contains("0"), "got: $message")
    }
}
