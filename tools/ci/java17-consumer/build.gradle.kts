import org.gradle.api.attributes.Category
import org.gradle.api.attributes.Usage
import org.gradle.api.attributes.java.TargetJvmVersion

plugins {
    kotlin("jvm") version "2.3.0"
    application
}

kotlin { jvmToolchain(17) }

val modules = listOf("jsonrpc4k-core", "jsonrpc4k-core-jvm", "jsonrpc4k-transport", "jsonrpc4k-transport-jvm")
val java16Probes =
    modules.associateWith { module ->
        configurations.create("java16-$module") {
            isCanBeConsumed = false
            isCanBeResolved = true
            attributes {
                attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.JAVA_RUNTIME))
                attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.LIBRARY))
                attribute(TargetJvmVersion.TARGET_JVM_VERSION_ATTRIBUTE, 16)
            }
        }
    }

dependencies {
    implementation("io.github.albertocavalcante:jsonrpc4k-core:${System.getenv("FIXTURE_VERSION")}")
    implementation("io.github.albertocavalcante:jsonrpc4k-transport:${System.getenv("FIXTURE_VERSION")}")
    implementation("io.github.albertocavalcante:jsonrpc4k-core-jvm:${System.getenv("FIXTURE_VERSION")}")
    implementation("io.github.albertocavalcante:jsonrpc4k-transport-jvm:${System.getenv("FIXTURE_VERSION")}")
    java16Probes.forEach { (module, configuration) ->
        add(configuration.name, "io.github.albertocavalcante:$module:${System.getenv("FIXTURE_VERSION")}")
    }
}

application { mainClass = "MainKt" }

tasks.register("verifyJava16Rejected") {
    doLast {
        java16Probes.forEach { (module, configuration) ->
            val failure =
                runCatching { configuration.resolve() }.exceptionOrNull()
                    ?: error("A Java 16 consumer unexpectedly resolved $module")
            val details = failure.stackTraceToString()
            val requested = "io.github.albertocavalcante:$module:${System.getenv("FIXTURE_VERSION")}"
            val noMatchingVariant =
                "No matching variant of $requested" in details && "Java 16" in details && "Java 17" in details
            val jvmTooNew =
                "JVM runtime version 16" in details &&
                    "'$requested' is only compatible with JVM runtime version 17" in details
            check(noMatchingVariant || jvmTooNew) {
                "Java 16 probe failed for an unrelated reason: $details"
            }
        }
    }
}
