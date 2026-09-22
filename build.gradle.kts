import com.vanniktech.maven.publish.MavenPublishBaseExtension
import org.gradle.kotlin.dsl.configure
import org.jetbrains.kotlin.gradle.dsl.KotlinJvmProjectExtension

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.plugin.serialization) apply false
    alias(libs.plugins.mavenPublish) apply false
}

allprojects {
    group = "net.kodein.kcpt"
    version = "1.0-SNAPSHOT"
}

// Every module needs the same toolchain, the same strict explicit-API mode, and the same set of
// experimental opt-ins (kotlinx.coroutines' own, plus this project's InternalKcptApi marker once
// kcpt-core declares it). Centralised here instead of copy-pasted into each module's build script,
// which is exactly the kind of setting that silently drifts once there is more than one of them.
val kotlinJvmPluginId = libs.plugins.kotlin.jvm.get().pluginId
subprojects {
    pluginManager.withPlugin(kotlinJvmPluginId) {
        extensions.configure<KotlinJvmProjectExtension> {
            jvmToolchain(findProperty("jvm.toolchain")!!.toString().toInt())
            explicitApi()

            compilerOptions {
                optIn.addAll(
                    "kotlin.concurrent.atomics.ExperimentalAtomicApi",
                    "kotlin.contracts.ExperimentalContracts",
                    "kotlin.time.ExperimentalTime",
                    "kotlin.uuid.ExperimentalUuidApi",
                    "kotlinx.coroutines.ExperimentalCoroutinesApi",
                    "net.kodein.kcpt.InternalKcptApi",
                )
            }
        }
    }
}

val mavenPublishPluginId = libs.plugins.mavenPublish.get().pluginId
subprojects {
    pluginManager.withPlugin(mavenPublishPluginId) {
        extensions.configure<MavenPublishBaseExtension> {
            pom {
                url = "https://github.com/kodein-koders/KCPT"
                licenses {
                    license {
                        name = "MIT License"
                        url = "https://opensource.org/licenses/MIT"
                        distribution = "repo"
                    }
                }
                issueManagement {
                    system.set("Github")
                    url.set("https://github.com/kodein-koders/KCPT/issues")
                }
                scm {
                    connection.set("https://github.com/kodein-koders/KCPT.git")
                    url.set("https://github.com/kodein-koders/KCPT")
                }
                developers {
                    developer {
                        name = "Kodein Koders"
                        email = "contact@kodein.net"
                        url = "https://kodein-koders.com"
                    }
                }
            }
        }
    }
}
