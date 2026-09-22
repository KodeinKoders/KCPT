package net.kodein.kcpt

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import kotlin.time.Instant
import kotlin.time.toJavaInstant
import kotlin.time.toKotlinInstant


/**
 * Runs [block] as one transaction on this connection: commits when it returns, rolls back when it
 * throws, and restores `autoCommit` either way so the connection goes back to the pool clean.
 * [block] may commit work of its own part-way through; what this guarantees is that whatever is
 * still uncommitted at the end is committed, or rolled back if [block] fails.
 *
 * **This call owns the connection's transaction, so it must not be nested on the same connection.**
 * A nested call would find `autoCommit` already `false`, and its commit — at the end of the *inner*
 * block — would commit the outer block's work along with it, well before the outer block is done.
 * Every call site holds to this by taking its own connection from [Source] and closing it again.
 *
 * It is worth saying why that is a rule here rather than something this function detects: an
 * `autoCommit` of `false` on entry means nothing on its own. It is equally what a pool configured
 * to hand out non-auto-commit connections gives every caller, which works correctly today, since
 * the commit below runs whatever `autoCommit` started as. Refusing those connections would reject a
 * legitimate setup outright, and treating them as an open transaction — skipping the commit and
 * letting the outermost caller decide — would be worse still: with no outer transaction to do it,
 * nothing would ever commit, and every write would be discarded when the connection was returned.
 */
@InternalKcptApi
public inline fun <R> Connection.transaction(
    block: Connection.() -> R
): R {
    contract {
        callsInPlace(block, InvocationKind.EXACTLY_ONCE)
    }

    val previousAutoCommit = autoCommit
    autoCommit = false

    var exception: Throwable? = null
    return try {
        block()
    } catch (e: Throwable) {
        exception = e
        try { rollback() } catch (s: Throwable) { e.addSuppressed(s) }
        throw e
    } finally {
        // Nested so that a failing commit still restores autoCommit, rather than handing a dirty
        // connection back to the pool.
        try {
            if (exception == null) {
                commit()
            }
        } catch (e: Throwable) {
            exception = e
            throw e
        } finally {
            try { autoCommit = previousAutoCommit } catch (s: Throwable) {
                exception?.addSuppressed(s) ?: throw s
            }
        }
    }
}

// pgjdbc has no java.time.Instant support in either direction; java.time.OffsetDateTime is the
// supported type for TIMESTAMPTZ. UTC is safe here since TIMESTAMPTZ stores an absolute instant
// and the offset is only a wire encoding.
@InternalKcptApi
public fun PreparedStatement.setInstant(parameterIndex: Int, value: Instant) {
    setObject(parameterIndex, value.toJavaInstant().atOffset(ZoneOffset.UTC))
}

@InternalKcptApi
public fun ResultSet.getInstantOrNull(columnIndex: Int): Instant? =
    getObject(columnIndex, OffsetDateTime::class.java)?.toInstant()?.toKotlinInstant()
