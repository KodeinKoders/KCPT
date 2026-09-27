package net.kodein.kcpt.cron

import net.kodein.kcpt.SchemaMigration


// The DDL a CronSink runs on its own unless created with manageSchema = false, for an application
// that would rather apply it itself. Pure DDL: the version bookkeeping in the properties table is
// only ever read by the sink's own migration, so it is left out.
public object CronSchema {
    // The schema version this release of KCPT expects its cron tables to be at.
    public val version: Int get() = CronSetup.version

    // Every step from version 0, in order; drop() the ones already applied. tablesPrefix must be
    // the same one the sink is given.
    public fun migrations(tablesPrefix: String = "public.kcpt_"): List<SchemaMigration> =
        CronSetup.migrations(CronTables.fromPrefix(tablesPrefix))
}
