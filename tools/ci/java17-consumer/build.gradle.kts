import org.gradle.api.attributes.Category
import org.gradle.api.attributes.Usage
import org.gradle.api.attributes.java.TargetJvmVersion

plugins {
    kotlin("jvm") version "2.3.0"
    application
}

kotlin { jvmToolchain(17) }

val java16Probe = configurations.create("java16Probe") {
    isCanBeConsumed = false
    isCanBeResolved = true
    attributes {
        attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
        attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
        attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 16)
    }
}

dependencies {
    implementation("io.github.albertocavalcante:jsonrpc4k-core:${System.getenv("FIXTURE_VERSION")}")
    implementation("io.github.albertocavalcante:jsonrpc4k-transport:${System.getenv("FIXTURE_VERSION")}")
    add(java16Probe.name, "io.github.albertocavalcante:jsonrpc4k-core:${System.getenv("FIXTURE_VERSION")}")
}

application { mainClass = "MainKt" }

tasks.register("verifyJava16Rejected") {
    doLast {
        check(runCatching { java16Probe.resolve() }.isFailure) {
            "A Java 16 consumer unexpectedly resolved the Java 17 artifact"
        }
    }
}
