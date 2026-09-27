package net.kodein.kcpt.cron

import kotlinx.coroutines.CoroutineScope
import net.kodein.kcpt.OnceInitializer
import net.kodein.kcpt.Source
import kotlin.time.Clock
import kotlin.time.Instant


public interface CronSink {
    public suspend fun initialize()

    // startAt and runImmediately only ever matter the first time this name is ever registered —
    // once the row exists, every later call (e.g. on every app restart) is a no-op against
    // whatever progress is already stored, via ON CONFLICT DO NOTHING in CronImpl.register().
    // They therefore live here as registration parameters, not in Cron.Config: Config is "how
    // this cron behaves from now on", re-supplied identically on every restart; these two are
    // "how its history was seeded", which only ever takes effect once.
    public suspend fun cron(
        name: String,
        config: Cron.Config,
        startAt: Instant = Clock.System.now(),
        runImmediately: Boolean = false,
    ): Cron
}

public fun CronSink(
    source: Source,
    tablesPrefix: String = "public.kcpt_",
    initializationScope: CoroutineScope? = null,
    // false leaves creating and migrating the tables to the application — see CronSchema for the
    // DDL to apply. initialize() then does nothing, and the properties table is never used.
    manageSchema: Boolean = true,
): CronSink = CronSinkImpl(
    source = source,
    tablesPrefix = tablesPrefix,
    initializationScope = initializationScope,
    manageSchema = manageSchema,
)

internal class CronSinkImpl(
    private val source: Source,
    tablesPrefix: String,
    initializationScope: CoroutineScope?,
    manageSchema: Boolean,
) : CronSink {
    private val tables = CronTables.fromPrefix(tablesPrefix)

    private val initializer = OnceInitializer(initializationScope) {
        if (manageSchema) context(tables) { CronSetup.initializeCronSchema(source) }
    }

    override suspend fun initialize() = initializer.initialize()

    override suspend fun cron(
        name: String,
        config: Cron.Config,
        startAt: Instant,
        runImmediately: Boolean,
    ): Cron {
        initialize()
        val cron = CronImpl(
            name = name,
            config = config,
            source = source,
            tables = tables,
            startAt = startAt,
            runImmediately = runImmediately,
        )
        cron.register()
        return cron
    }
}
