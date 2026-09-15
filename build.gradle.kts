plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.plugin.serialization)
}

group = "net.kodein.mq.kcpmq"
version = "1.0-SNAPSHOT"

kotlin {
    explicitApi()
}

dependencies {
    implementation(libs.kotlinx.serialization.json)
    implementation(libs.kotlinx.coroutines)
    implementation(libs.postgresql)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.kotlin.test.junit)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.hikaricp)
    testImplementation(libs.testcontainers)
    testImplementation(libs.testcontainers.postgresql)
}

kotlin {
    jvmToolchain(21)
}
