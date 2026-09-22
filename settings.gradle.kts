dependencyResolutionManagement {
    @Suppress("UnstableApiUsage")
    repositories {
        mavenCentral()
    }
}

plugins {
    // Downloads the JDKs the subprojects' toolchains ask for. Pinned here rather than in
    // gradle/libs.versions.toml because a settings plugin can't resolve a version-catalog alias -
    // the catalog isn't built yet at this point.
    // https://github.com/gradle/foojay-toolchains/releases
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

rootProject.name = "KCPT"

enableFeaturePreview("TYPESAFE_PROJECT_ACCESSORS")

include(
    ":kcpt-core",
    ":kcpt-message-queue",
    ":kcpt-cron",
)
