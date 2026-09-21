@file:Suppress("SqlSourceToSinkFlow")

package net.kodein.mq.kcpmq

import kotlinx.coroutines.test.runTest
import org.junit.Test
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

// An index that stops serving the query it was made for fails silently: nothing breaks, deliveries
// just start scanning the whole table. These tests read the plan itself, which is the only place
// that shows up before it shows up in production.
class QueryPlanTest : AbstractPgsqlTest() {

    // Enough rows for the planner to have an opinion. Below a few thousand a sequential scan really
    // is cheaper, and the assertion would be measuring the row count rather than the indexes.
    private val rows = 3_000

    @Test
    fun theSelectionQueryIsServedByAnIndexRatherThanAScan() = runTest(timeout = 30.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.bulkInsertMessages(prefix, queue, rows)
        dataSource.analyze(prefix)

        // Kept in step with receiveNextOrNull's statement by hand — if that query changes, this one
        // has to change with it, or this test goes on approving a plan for SQL nobody runs.
        // Values are inlined rather than bound: a parameterised statement can be planned
        // generically, which is not the plan the real query gets.
        val plan = dataSource.explain(
            """
                SELECT id, payload, notes, started_at
                FROM ${prefix}messages
                WHERE
                        queue = '$queue'
                    AND due_at <= now()
                    AND (started_at IS NULL OR started_at < now() - interval '1 minute')
                ORDER BY due_at, id
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            """.trimIndent()
        )

        val rendered = plan.joinToString("\n")
        assertTrue(
            plan.any { "Index Scan" in it },
            "the selection query must be served by an index; plan was:\n$rendered",
        )
        assertTrue(
            plan.none { "Seq Scan" in it },
            "the selection query must not scan the whole table; plan was:\n$rendered",
        )
    }

    @Test
    fun theWakeUpQueryHasStartedSecondSubqueryIsServedByAnIndexRatherThanAScan() = runTest(timeout = 30.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.bulkInsertMessages(prefix, queue, rows, started = true)
        dataSource.analyze(prefix)

        // The wake-up query's other subquery, min(due_at) WHERE started_at IS NULL, is already
        // served by the partial index built for it. Kept in step with MessageQueue.kt by hand, as
        // the selection query above is.
        val plan = dataSource.explain(
            """
                SELECT min(started_at)
                FROM ${prefix}messages
                WHERE
                        queue = '$queue'
                    AND started_at IS NOT NULL
                    AND started_at > now() - interval '1 hour'
            """.trimIndent()
        )

        val rendered = plan.joinToString("\n")
        assertTrue(
            plan.any { "Index Scan" in it || "Index Only Scan" in it },
            "the wake-up query's started_at subquery must be served by an index; plan was:\n$rendered",
        )
        assertTrue(
            plan.none { "Seq Scan" in it },
            "the wake-up query's started_at subquery must not scan the whole table; plan was:\n$rendered",
        )
    }
}
