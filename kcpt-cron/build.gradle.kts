plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.mavenPublish)
}

dependencies {
    api(projects.kcptCore)

    // AbstractPgsqlTest and the generic fixtures (rawNotify, asSource, ...) live in kcpt-core's
    // testFixtures, not its main source — a project dependency's api/implementation configuration
    // never pulls in another module's testFixtures variant on its own, so this module's own test
    // source set needs it named explicitly.
    testImplementation(testFixtures(projects.kcptCore))
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.kotlinx.coroutines.test)

    // Needed for exactly one test — CronSchemaSetupTest, which proves a MessageQueueSink and a
    // CronSink coexist independently on one prefix. That's the one place a feature module's tests
    // legitimately need the *other* feature module.
    testImplementation(projects.kcptMessageQueue)
    testImplementation(testFixtures(projects.kcptMessageQueue))
}

mavenPublishing {
    pom {
        name = "kcpt-cron"
        description = "PostgreSQL-backed cron scheduling for Kotlin coroutines, on the same primitives as kcpt-message-queue"
    }
}

tasks.test {
    // Each fork builds its own Testcontainers Postgres in AbstractPgsqlTest's companion object, so
    // forks share no database and cannot exhaust one another's connections — the cost is one
    // container per fork, which is why this is half the cores rather than all of them.
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
}
