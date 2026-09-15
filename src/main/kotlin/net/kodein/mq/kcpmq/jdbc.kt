package net.kodein.mq.kcpmq

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlin.contracts.ExperimentalContracts
import kotlin.contracts.InvocationKind
import kotlin.contracts.contract
import kotlin.time.ExperimentalTime
import kotlin.time.Instant
import kotlin.time.toJavaInstant
import kotlin.time.toKotlinInstant


@OptIn(ExperimentalContracts::class)
internal inline fun <R> Connection.transaction(
    commit: Boolean = true,
    block: Connection.() -> R
): R {
    contract {
        callsInPlace(block, InvocationKind.EXACTLY_ONCE)
    }
    val previousAutoCommit = autoCommit
    autoCommit = false
    try {
        val ret = block()
        if (commit) {
            commit()
        }
        return ret
    } catch (e: Throwable) {
        try { rollback() }
        catch (_: Throwable) {}
        throw e
    } finally {
        try { autoCommit = previousAutoCommit }
        catch (_: Throwable) {}
    }
}

// pgjdbc has no java.time.Instant support in either direction; java.time.OffsetDateTime is the
// supported type for TIMESTAMPTZ. UTC is safe here since TIMESTAMPTZ stores an absolute instant
// and the offset is only a wire encoding.
@OptIn(ExperimentalTime::class)
internal fun PreparedStatement.setInstant(parameterIndex: Int, value: Instant) {
    setObject(parameterIndex, value.toJavaInstant().atOffset(ZoneOffset.UTC))
}

@OptIn(ExperimentalTime::class)
internal fun ResultSet.getInstantOrNull(columnIndex: Int): Instant? =
    getObject(columnIndex, OffsetDateTime::class.java)?.toInstant()?.toKotlinInstant()
