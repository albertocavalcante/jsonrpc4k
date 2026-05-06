plugins {
    id("jsonrpc4k.multiplatform")
    id("publishing-convention")
}

kotlin {
    sourceSets {
        commonMain {
            dependencies {
                api(projects.jsonrpc4kCore)
                api(libs.kotlinx.io.core)
                api(libs.kotlinx.coroutines.core)
            }
        }

        commonTest {
            dependencies {
                implementation(libs.kotlin.test)
                implementation(libs.kotest.assertions.core)
                implementation(libs.kotlinx.coroutines.test)
            }
        }

        jvmTest {
            dependencies {
                implementation(libs.kotest.runner.junit5)
            }
        }

        // Create native source set hierarchy manually
        // The default hierarchy template doesn't support custom intermediate groups like posixMain
        val nativeMain by creating {
            dependsOn(commonMain.get())
        }

        val posixMain by creating {
            dependsOn(nativeMain)
        }

        // Apple targets depend on posixMain
        macosX64Main {
            dependsOn(posixMain)
        }
        macosArm64Main {
            dependsOn(posixMain)
        }

        // Linux targets depend on posixMain
        linuxX64Main {
            dependsOn(posixMain)
        }
        linuxArm64Main {
            dependsOn(posixMain)
        }

        // Windows depends on nativeMain directly
        mingwX64Main {
            dependsOn(nativeMain)
        }
    }
}
