package net.kodein.mq.kcpmq

import org.junit.Test
import java.lang.reflect.InvocationHandler
import java.lang.reflect.InvocationTargetException
import java.lang.reflect.Proxy
import java.sql.Connection
import java.sql.SQLException
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

// Connection.transaction is the only place that decides whether work is committed or rolled back,
// and its failure paths are hard to reach through a real database, so they are driven here with a
// fake Connection that can be made to fail on demand.
class JdbcTransactionTest {

    private class FakeConnection(
        val failCommit: Boolean = false,
        val failAutoCommitRestore: Boolean = false,
    ) {
        val calls = mutableListOf<String>()
        private var autoCommit = true

        val connection: Connection = Proxy.newProxyInstance(
            Connection::class.java.classLoader,
            arrayOf(Connection::class.java),
            InvocationHandler { _, method, args ->
                when (method.name) {
                    "getAutoCommit" -> autoCommit
                    "setAutoCommit" -> {
                        val target = args!![0] as Boolean
                        calls += "setAutoCommit($target)"
                        // Only the restore back to the original value is made to fail.
                        if (failAutoCommitRestore && target) throw SQLException("restore failed")
                        autoCommit = target
                        null
                    }
                    "commit" -> {
                        calls += "commit"
                        if (failCommit) throw SQLException("commit failed")
                        null
                    }
                    "rollback" -> {
                        calls += "rollback"
                        null
                    }
                    else -> null
                }
            }
        ) as Connection
    }

    private inline fun <R> runOn(fake: FakeConnection, noinline block: Connection.() -> R): R =
        try {
            fake.connection.transaction(block)
        } catch (e: InvocationTargetException) {
            // Unwrap anything the proxy rethrows.
            throw e.targetException
        }

    @Test
    fun aSuccessfulBlockCommitsAndRestoresAutoCommit() {
        val fake = FakeConnection()

        val result = runOn(fake) { "ok" }

        assertEquals("ok", result)
        assertContentEquals(listOf("setAutoCommit(false)", "commit", "setAutoCommit(true)"), fake.calls)
    }

    @Test
    fun aThrowingBlockRollsBackAndDoesNotCommit() {
        val fake = FakeConnection()

        val thrown = assertFailsWith<IllegalStateException> {
            runOn(fake) { throw IllegalStateException("block failed") }
        }

        assertEquals("block failed", thrown.message)
        assertContentEquals(listOf("setAutoCommit(false)", "rollback", "setAutoCommit(true)"), fake.calls)
    }

    @Test
    fun aFailingCommitPropagatesAndStillRestoresAutoCommit() {
        val fake = FakeConnection(failCommit = true)

        val thrown = assertFailsWith<SQLException> { runOn(fake) { "ok" } }

        assertEquals("commit failed", thrown.message)
        assertTrue(
            fake.calls.contains("setAutoCommit(true)"),
            "autoCommit must be restored even when the commit fails, or the pool gets a dirty connection"
        )
    }

    // The case the nested catch exists for: when both the commit and the autoCommit restore fail,
    // the commit failure is the one worth surfacing — the restore failure rides along suppressed.
    @Test
    fun aFailingCommitWinsOverAFailingAutoCommitRestore() {
        val fake = FakeConnection(failCommit = true, failAutoCommitRestore = true)

        val thrown = assertFailsWith<SQLException> { runOn(fake) { "ok" } }

        assertEquals("commit failed", thrown.message)
        assertTrue(
            thrown.suppressed.any { it.message == "restore failed" },
            "the restore failure must be suppressed onto the commit failure, not replace it"
        )
    }

    @Test
    fun aFailingBlockWinsOverAFailingAutoCommitRestore() {
        val fake = FakeConnection(failAutoCommitRestore = true)

        val thrown = assertFailsWith<IllegalStateException> {
            runOn(fake) { throw IllegalStateException("block failed") }
        }

        assertEquals("block failed", thrown.message)
        assertTrue(
            thrown.suppressed.any { it.message == "restore failed" },
            "the restore failure must not mask the original block failure"
        )
    }
}
