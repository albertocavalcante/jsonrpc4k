plugins {
    id("jsonrpc4k.multiplatform")
    id("publishing-convention")
}

// A variable that only the build sets, so a test can prove that a null environment value removes
// it from the child; PATH is unusable because a Windows child still saw it after removal.
tasks.withType<Test>().configureEach {
    environment("JSONRPC4K_PARENT_MARKER", "parent")
}

kotlin {
    js {
        // The JS transports load Node modules lazily, so a browser bundle can import the
        // library; the transport tests exercise Node behavior, so they run on Node only.
        browser {
            testTask {
                enabled = false
            }
        }
    }

    sourceSets {
        commonMain {
            dependencies {
                api(projects.jsonrpc4kCore)
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

        // Native test hierarchy mirrors the main one: nativeTest <- posixTest <- {macos*, linux*}Test.
        val nativeTest by creating {
            dependsOn(commonTest.get())
        }

        val posixTest by creating {
            dependsOn(nativeTest)
        }

        macosX64Test {
            dependsOn(posixTest)
        }
        macosArm64Test {
            dependsOn(posixTest)
        }
        linuxX64Test {
            dependsOn(posixTest)
        }
        linuxArm64Test {
            dependsOn(posixTest)
        }
        mingwX64Test {
            dependsOn(nativeTest)
        }

        // Create native source set hierarchy manually
        // The default hierarchy template doesn't support custom intermediate groups like posixMain
        val nativeMain by creating {
            dependsOn(commonMain.get())
        }

        val posixMain by creating {
            dependsOn(nativeMain)
        }

        // poll() takes a platform-width nfds_t, so its call lives in per-family source sets.
        val posixAppleMain by creating {
            dependsOn(posixMain)
        }
        val posixLinuxMain by creating {
            dependsOn(posixMain)
        }

        // Apple targets depend on posixAppleMain
        macosX64Main {
            dependsOn(posixAppleMain)
        }
        macosArm64Main {
            dependsOn(posixAppleMain)
        }

        // Linux targets depend on posixLinuxMain
        linuxX64Main {
            dependsOn(posixLinuxMain)
        }
        linuxArm64Main {
            dependsOn(posixLinuxMain)
        }

        // Windows depends on nativeMain directly
        mingwX64Main {
            dependsOn(nativeMain)
        }
    }
}
