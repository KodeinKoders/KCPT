package net.kodein.mq.kcpmq

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


internal inline fun <R> Connection.transaction(
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
internal fun PreparedStatement.setInstant(parameterIndex: Int, value: Instant) {
    setObject(parameterIndex, value.toJavaInstant().atOffset(ZoneOffset.UTC))
}

internal fun ResultSet.getInstantOrNull(columnIndex: Int): Instant? =
    getObject(columnIndex, OffsetDateTime::class.java)?.toInstant()?.toKotlinInstant()
