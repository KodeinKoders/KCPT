package net.kodein.kcpt.cron

import org.junit.Test
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
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
}
