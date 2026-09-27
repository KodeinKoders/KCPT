package net.kodein.kcpt.mq

import net.kodein.kcpt.HasPropertiesTable
import net.kodein.kcpt.validatedTableName

@ConsistentCopyVisibility
internal data class MessageQueueTables private constructor(
    override val properties: String,
    val messages: String,
) : HasPropertiesTable {
    companion object {
        fun named(
            properties: String,
            messages: String,
        ) = MessageQueueTables(
            properties = validatedTableName(properties),
            messages = validatedTableName(messages),
        )

        // The one place a prefix becomes table names, shared by the sink and MessageQueueSchema so
        // the tables it uses and the scripts it publishes can never disagree.
        fun fromPrefix(tablesPrefix: String) = named(
            properties = "${tablesPrefix}properties",
            messages = "${tablesPrefix}messages",
        )
    }
}
