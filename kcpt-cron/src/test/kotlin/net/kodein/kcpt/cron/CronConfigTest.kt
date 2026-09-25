package net.kodein.kcpt.cron

import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

// Every one of these settings fails silently rather than loudly when out of range — a cron that
// never catches up, a hot spin, a lease reclaimed out from under its own holder — so they are
// rejected at construction instead.
class CronConfigTest {

    @Test
    fun theDefaultsAreValid() {
        Cron.Config(interval = 1.minutes)
    }

    @Test
    fun aNonPositiveIntervalIsRejected() {
        assertFailsWith<IllegalArgumentException> { Cron.Config(interval = Duration.ZERO) }
        assertFailsWith<IllegalArgumentException> { Cron.Config(interval = (-1).seconds) }
    }

    // Below interval, every normal (non-catch-up) tick would cover less ground than the cadence
    // it advances by, so the cron falls permanently further behind and never catches up.
    @Test
    fun aMaxIntervalBelowIntervalIsRejected() {
        assertFailsWith<IllegalArgumentException> {
            Cron.Config(interval = 10.minutes, maxInterval = 9.minutes)
        }
        Cron.Config(interval = 10.minutes, maxInterval = 10.minutes)
    }

    @Test
    fun theDefaultMaxIntervalIsFiftyTimesInterval() {
        assertEquals(500.minutes, Cron.Config(interval = 10.minutes).maxInterval)
    }

    @Test
    fun theDefaultWindowEndIsExclusive() {
        assertEquals(Cron.WindowEnd.EXCLUSIVE, Cron.Config(interval = 1.minutes).windowEnd)
    }

    @Test
    fun negativeRetriesIsRejected() {
        assertFailsWith<IllegalArgumentException> { Cron.Config(interval = 1.minutes, retries = -1) }
        Cron.Config(interval = 1.minutes, retries = 0) // dead-letter on first failure is meaningful
    }

    @Test
    fun aNonPositiveTickTimeoutIsRejected() {
        assertFailsWith<IllegalArgumentException> {
            Cron.Config(interval = 1.minutes, tickTimeout = Duration.ZERO)
        }
        assertFailsWith<IllegalArgumentException> {
            Cron.Config(interval = 1.minutes, tickTimeout = (-1).seconds)
        }
    }

    @Test
    fun aTickGraceBelowTheMinimumIsRejected() {
        assertFailsWith<IllegalArgumentException> {
            Cron.Config(interval = 1.minutes, tickGrace = (-1).milliseconds)
        }
        assertFailsWith<IllegalArgumentException> {
            Cron.Config(interval = 1.minutes, tickGrace = Duration.ZERO)
        }
        assertFailsWith<IllegalArgumentException> {
            Cron.Config(interval = 1.minutes, tickGrace = Cron.Config.MIN_TICK_GRACE - 1.milliseconds)
        }
        Cron.Config(interval = 1.minutes, tickGrace = Cron.Config.MIN_TICK_GRACE)
    }

    // A short tickTimeout would otherwise derive a margin below the minimum from the 20% default
    // and fail its own construction.
    @Test
    fun theDerivedTickGraceIsFlooredAtTheMinimum() {
        assertEquals(
            Cron.Config.MIN_TICK_GRACE,
            Cron.Config(interval = 1.minutes, tickTimeout = 100.milliseconds).tickGrace,
        )
    }
}
