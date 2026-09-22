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
    }
}
