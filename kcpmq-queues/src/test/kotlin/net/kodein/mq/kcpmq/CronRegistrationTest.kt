package net.kodein.mq.kcpmq

import kotlinx.coroutines.test.runTest
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Clock
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import kotlin.time.toKotlinInstant

class CronRegistrationTest : AbstractPgsqlTest() {

    private fun sink(prefix: String) = CronSink(source = dataSource.asSource(), tablesPrefix = prefix)

    @Test
    fun defaultRegistrationSeedsNextSlotAtNow() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val before = Clock.System.now()
        sink(prefix).cron("nightly", Cron.Config(interval = 10.minutes))
        val after = Clock.System.now()

        val nextSlotAt = dataSource.cronNextSlotAtOf(prefix, "nightly")!!.toInstant().toKotlinInstant()
        assertTrue(nextSlotAt in before..after, "next_slot_at should be seeded at ~now, was $nextSlotAt")
    }

    @Test
    fun startAtInThePastSeedsNextSlotAtThere() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val start = Clock.System.now() - 3.minutes
        sink(prefix).cron("catchup", Cron.Config(interval = 10.minutes), startAt = start)

        val nextSlotAt = dataSource.cronNextSlotAtOf(prefix, "catchup")!!.toInstant().toKotlinInstant()
        assertEquals(start.epochSeconds, nextSlotAt.epochSeconds)
    }

    // runImmediately seeds the cursor one interval *early*, so the materialization gate
    // (now >= next_slot_at + interval) is already satisfied as soon as now == startAt, instead of
    // only after a full extra interval has passed.
    @Test
    fun runImmediatelySeedsNextSlotAtOneIntervalEarly() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val start = Clock.System.now()
        sink(prefix).cron("kickoff", Cron.Config(interval = 10.minutes), startAt = start, runImmediately = true)

        val nextSlotAt = dataSource.cronNextSlotAtOf(prefix, "kickoff")!!.toInstant().toKotlinInstant()
        assertEquals((start - 10.minutes).epochSeconds, nextSlotAt.epochSeconds)
    }

    @Test
    fun reRegisteringAnExistingNameLeavesStoredProgressUntouched() = runTest(timeout = 5.seconds) {
        val prefix = uniquePrefix()
        val theSink = sink(prefix)
        val originalStart = Clock.System.now() - 1.minutes
        theSink.cron("restart-me", Cron.Config(interval = 10.minutes), startAt = originalStart)
        val originalNextSlotAt = dataSource.cronNextSlotAtOf(prefix, "restart-me")!!.toInstant().toKotlinInstant()

        // Simulates a restart: the app calls cron(...) again with a fresh startAt.
        theSink.cron("restart-me", Cron.Config(interval = 10.minutes), startAt = Clock.System.now())

        val nextSlotAtAfterRestart = dataSource.cronNextSlotAtOf(prefix, "restart-me")!!.toInstant().toKotlinInstant()
        assertEquals(originalNextSlotAt.epochSeconds, nextSlotAtAfterRestart.epochSeconds)
    }
}
