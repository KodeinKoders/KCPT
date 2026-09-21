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

    // Every per-message statement (successDelete, extend, the dead-letter deletes, ...) matches
    // queue = ? AND id = ?, and the primary key is (queue, id) — kept in step with Message.kt's
    // successDelete by hand, which all the others share the shape of. The id-only alternative is
    // not what production runs any more; it is planned only for comparison, to show the shape of
    // the regression this guards against. On PostgreSQL 18 the planner can cover an id-only match
    // with a skip scan, but that is new in this release and this project pins no minimum server
    // version — on 17 and earlier the same statement is a sequential scan.
    @Test
    fun successDeletesStatementIsAPrimaryKeyLookupRatherThanAnIdOnlyMatch() = runTest(timeout = 30.seconds) {
        val prefix = uniquePrefix()
        val queue = uniqueQueueName()
        MessageQueueSink(source = dataSource.asSource(), tablesPrefix = prefix).initialize()

        dataSource.bulkInsertMessages(prefix, queue, rows)
        dataSource.analyze(prefix)
        val id = kotlin.uuid.Uuid.random()

        val queueAndId = dataSource.explain(
            "DELETE FROM ${prefix}messages WHERE queue = '$queue' AND id = '$id'"
        ).joinToString("\n")
        val idOnly = dataSource.explain(
            "DELETE FROM ${prefix}messages WHERE id = '$id'"
        ).joinToString("\n")

        assertTrue(
            ("Index Scan" in queueAndId || "Index Only Scan" in queueAndId) && "Seq Scan" !in queueAndId,
            "successDelete's statement must be a direct index lookup; plan was:\n$queueAndId",
        )

        // cost=startup..total — startup is identical for both (the index root descent), so it is
        // the total that has to be compared.
        fun totalCost(plan: String) = plan.lineSequence().first()
            .substringAfter("..").substringBefore(" ").toDouble()

        assertTrue(
            totalCost(queueAndId) < totalCost(idOnly),
            "matching queue alongside id must be cheaper than id alone; " +
                "queue+id plan was:\n$queueAndId\n\nid-only plan was:\n$idOnly",
        )
    }
}
