import com.vanniktech.maven.publish.MavenPublishBaseExtension
import org.gradle.kotlin.dsl.configure

plugins {
    alias(libs.plugins.kotlin.jvm) apply false
    alias(libs.plugins.kotlin.plugin.serialization) apply false
    alias(libs.plugins.mavenPublish) apply false
}

allprojects {
    group = "net.kodein.mq.kcpmq"
    version = "1.0-SNAPSHOT"
}

val mavenPublishPluginId = libs.plugins.mavenPublish.get().pluginId
subprojects {
    pluginManager.withPlugin(mavenPublishPluginId) {
        extensions.configure<MavenPublishBaseExtension> {
            pom {
                url = "https://github.com/kodein-koders/KCPMQ"
                licenses {
                    license {
                        name = "MIT License"
                        url = "https://opensource.org/licenses/MIT"
                        distribution = "repo"
                    }
                }
                issueManagement {
                    system.set("Github")
                    url.set("https://github.com/kodein-koders/KCPMQ/issues")
                }
                scm {
                    connection.set("https://github.com/kodein-koders/KCPMQ.git")
                    url.set("https://github.com/kodein-koders/KCPMQ")
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
