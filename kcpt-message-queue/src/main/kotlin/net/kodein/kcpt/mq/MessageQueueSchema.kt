package net.kodein.kcpt.mq

import net.kodein.kcpt.SchemaMigration


// The DDL a MessageQueueSink runs on its own unless created with manageSchema = false, for an
// application that would rather apply it itself. Pure DDL: the version bookkeeping in the
// properties table is only ever read by the sink's own migration, so it is left out.
public object MessageQueueSchema {
    // The schema version this release of KCPT expects its message-queue tables to be at.
    public val version: Int get() = MessageQueueSetup.version

    // Every step from version 0, in order; drop() the ones already applied. tablesPrefix must be
    // the same one the sink is given.
    public fun migrations(tablesPrefix: String = "public.kcpt_"): List<SchemaMigration> =
        MessageQueueSetup.migrations(MessageQueueTables.fromPrefix(tablesPrefix))
}
