import org.gradle.api.attributes.java.TargetJvmVersion

plugins {
    kotlin("multiplatform")
    kotlin("plugin.serialization")
}

kotlin {
    explicitApi()

    jvm {
        attributes.attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 17)
        testRuns["test"].executionTask.configure {
            useJUnitPlatform()
        }
    }

    jvmToolchain(17)

    js(IR) {
        browser()
        nodejs()
    }

    // Native targets
    macosX64()
    macosArm64()
    linuxX64()
    linuxArm64()
    mingwX64 {
        // Windows reserves 1 MB of stack by default (8 MB on Linux/macOS). kotlinx.serialization
        // recurses per JSON nesting level, and depth 256 overflows the default reservation.
        binaries.all {
            linkerOpts("-Wl,--stack,8388608")
        }
    }

    compilerOptions {
        freeCompilerArgs.addAll(
            "-Xexpect-actual-classes",
            "-opt-in=kotlin.ExperimentalStdlibApi",
            "-opt-in=kotlinx.serialization.ExperimentalSerializationApi",
        )
    }
}
