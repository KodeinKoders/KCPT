package net.kodein.kcpt.cron

import kotlinx.coroutines.delay
import kotlinx.coroutines.test.runTest
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Instant

// windowHasElapsed's whole reason to exist as its own function: a single-instant boundary
// difference between EXCLUSIVE and INCLUSIVE isn't something a real wall-clock integration test
// could ever pin down, so it's tested directly here instead.
class CronWindowEndTest {

    private val to = Instant.parse("2024-01-01T00:00:00Z")

    @Test
    fun exclusiveHasElapsedOnceNowReachesTo() {
        assertFalse(windowHasElapsed(to - 1.milliseconds, to, Cron.WindowEnd.EXCLUSIVE))
        assertTrue(windowHasElapsed(to, to, Cron.WindowEnd.EXCLUSIVE))
        assertTrue(windowHasElapsed(to + 1.milliseconds, to, Cron.WindowEnd.EXCLUSIVE))
    }

    @Test
    fun inclusiveHasElapsedOnlyStrictlyPastTo() {
        assertFalse(windowHasElapsed(to - 1.milliseconds, to, Cron.WindowEnd.INCLUSIVE))
        assertFalse(windowHasElapsed(to, to, Cron.WindowEnd.INCLUSIVE))
        assertTrue(windowHasElapsed(to + 1.milliseconds, to, Cron.WindowEnd.INCLUSIVE))
    }

    // steadyStateWindowTo's whole reason to exist as its own function, same as windowHasElapsed
    // above: directly unit-testable without a database.
    private val from = Instant.parse("2024-01-01T00:00:00Z")
    private val defaultTo = from + 10.minutes

    @Test
    fun noShortenWindowKeepsTheDefaultTo() = runTest {
        assertEquals(defaultTo, steadyStateWindowTo(from, defaultTo) { _, to -> to })
    }

    @Test
    fun aShorterProposalIsUsedAsIs() = runTest {
        val proposed = from + 4.minutes
        assertEquals(proposed, steadyStateWindowTo(from, defaultTo) { _, _ -> proposed })
    }

    @Test
    fun aLongerProposalIsClampedToDefaultTo() = runTest {
        val proposed = defaultTo + 5.minutes
        assertEquals(defaultTo, steadyStateWindowTo(from, defaultTo) { _, _ -> proposed })
    }

    @Test
    fun aNonPositiveLengthProposalThrows() = runTest {
        assertFailsWith<IllegalArgumentException> {
            steadyStateWindowTo(from, defaultTo) { from, _ -> from }
        }
        assertFailsWith<IllegalArgumentException> {
            steadyStateWindowTo(from, defaultTo) { from, _ -> from - 1.minutes }
        }
    }

    @Test
    fun shortenWindowCanGenuinelySuspend() = runTest {
        val proposed = from + 4.minutes
        val result = steadyStateWindowTo(from, defaultTo) { _, _ ->
            delay(1.milliseconds)
            proposed
        }
        assertEquals(proposed, result)
    }
}
