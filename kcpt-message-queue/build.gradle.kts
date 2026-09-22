plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.serialization)
    alias(libs.plugins.mavenPublish)
    `java-test-fixtures`
}

dependencies {
    api(projects.kcptCore)

    // AbstractPgsqlTest and the generic fixtures (rawNotify, asSource, ...) live in kcpt-core's
    // testFixtures, not its main source — a project dependency's api/implementation configuration
    // never pulls in another module's testFixtures variant on its own, so this module's own test
    // and testFixtures source sets each need it named explicitly.
    testImplementation(testFixtures(projects.kcptCore))
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.kotlinx.coroutines.test)

    // The serialization plugin above is needed for this module's own testFixtures, which declare
    // @Serializable data classes (User, UserV2) to post through a MessageQueue<T>.
    testFixturesImplementation(testFixtures(projects.kcptCore))
    testFixturesImplementation(libs.kotlinx.serialization.json)
}

mavenPublishing {
    pom {
        name = "kcpt-message-queue"
        description = "PostgreSQL-backed message queues for Kotlin coroutines"
    }
}

tasks.test {
    // Each fork builds its own Testcontainers Postgres in AbstractPgsqlTest's companion object, so
    // forks share no database and cannot exhaust one another's connections — the cost is one
    // container per fork, which is why this is half the cores rather than all of them.
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
}
