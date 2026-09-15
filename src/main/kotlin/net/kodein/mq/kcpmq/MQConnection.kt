package net.kodein.mq.kcpmq

import org.postgresql.core.Utils


@ConsistentCopyVisibility
internal data class MQTables private constructor(
    val properties: String,
    val messages: String,
) {
    companion object {
        fun named(
            properties: String,
            messages: String,
        ) = MQTables(
            properties = Utils.escapeLiteral(null, properties, true).toString(),
            messages = Utils.escapeLiteral(null, messages, true).toString(),
        )
    }
}
