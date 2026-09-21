plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.serialization)
    alias(libs.plugins.mavenPublish)
}

kotlin {
    jvmToolchain(findProperty("jvm.toolchain")!!.toString().toInt())
    explicitApi()

    compilerOptions {
        optIn.addAll(
            "kotlin.concurrent.atomics.ExperimentalAtomicApi",
            "kotlin.contracts.ExperimentalContracts",
            "kotlin.time.ExperimentalTime",
            "kotlin.uuid.ExperimentalUuidApi",
            "kotlinx.coroutines.ExperimentalCoroutinesApi",
        )
    }
}

dependencies {
    // Flow and CoroutineScope appear in this module's own public signatures
    // (PGChannel.ConnectionFlow, openListenConnection), so they belong in its published POM's
    // compile scope.
    api(libs.kotlinx.coroutines)
    // KSerializer (PGChannel's constructor) and JsonElement (Message.notes) appear in this
    // module's own public signatures.
    api(libs.kotlinx.serialization)
    api(libs.kotlinx.serialization.json)

    // The driver is reached only through java.sql/javax.sql types in the public API; the one
    // PostgreSQL-specific use (LISTEN/NOTIFY through PGConnection) stays internal.
    implementation(libs.postgresql)

    testImplementation(libs.hikaricp)
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.postgresql)
}

mavenPublishing {
    pom {
        name = "kcpmq-queues"
        description = "PostgreSQL-backed message queues for Kotlin coroutines"
    }
}

tasks.test {
    // Each fork builds its own Testcontainers Postgres in AbstractPgsqlTest's companion object, so
    // forks share no database and cannot exhaust one another's connections — the cost is one
    // container per fork, which is why this is half the cores rather than all of them. Tests no
    // longer clean up after each other, so nothing here depends on running sequentially.
    maxParallelForks = (Runtime.getRuntime().availableProcessors() / 2).coerceAtLeast(1)
}
