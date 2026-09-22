package net.kodein.kcpt.cron

import net.kodein.kcpt.HasPropertiesTable
import net.kodein.kcpt.validatedTableName

@ConsistentCopyVisibility
internal data class CronTables private constructor(
    override val properties: String,
    val crons: String,
    val cronTicks: String,
) : HasPropertiesTable {
    companion object {
        fun named(
            properties: String,
            crons: String,
            cronTicks: String,
        ) = CronTables(
            properties = validatedTableName(properties),
            crons = validatedTableName(crons),
            cronTicks = validatedTableName(cronTicks),
        )
    }
}
