plugins {
    alias(libs.plugins.kotlin.jvm)
    // Needed only for this module's own test source: PGChannelTest declares a @Serializable data
    // class (Foo) to drive PGChannel<T> generically. Main source never declares a serializable
    // type of its own — kcptJson's callers bring their own — so this plugin has no effect there.
    alias(libs.plugins.kotlin.plugin.serialization)
    alias(libs.plugins.mavenPublish)
    `java-test-fixtures`
}

dependencies {
    // Flow and CoroutineScope appear in this module's own public signatures (PGChannel's
    // ConnectionFlow, openListenConnection), so they belong in its published POM's compile scope.
    api(libs.kotlinx.coroutines)
    // KSerializer (PGChannel's constructor) and JsonElement (kcptJson's callers) appear in this
    // module's own public signatures.
    api(libs.kotlinx.serialization)
    api(libs.kotlinx.serialization.json)

    // The driver is reached only through java.sql/javax.sql types in the public API; the one
    // PostgreSQL-specific use (LISTEN/NOTIFY through PGConnection) stays internal.
    implementation(libs.postgresql)

    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.kotlinx.coroutines.test)

    // testFixturesApi, not testFixturesImplementation: kcpt-message-queue and kcpt-cron consume
    // AbstractPgsqlTest through testFixtures(projects.kcptCore) and need a complete, working test
    // runtime for it without redeclaring Hikari/Testcontainers/kotlin-test themselves. Hikari in
    // particular is not an implementation detail here — AbstractPgsqlTest.dataSource is typed
    // HikariDataSource, so every subclass genuinely needs that type on its own compile classpath.
    testFixturesApi(libs.hikaricp)
    testFixturesApi(libs.kotlin.test.junit)
    testFixturesApi(libs.kotlinx.coroutines.test)
    testFixturesApi(libs.testcontainers)
    testFixturesApi(libs.testcontainers.postgresql)
}

mavenPublishing {
    pom {
        name = "kcpt-core"
        description = "Shared PostgreSQL + Kotlin coroutine primitives underlying kcpt-message-queue and kcpt-cron"
    }
}

tasks.test {
    // Each fork builds its own Testcontainers Postgres in AbstractPgsqlTest's companion object, so
    // forks share no database and cannot exhaust one another's connections — the cost is one
    // container per fork, which is why this is half the cores rather than all of them.
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
}
