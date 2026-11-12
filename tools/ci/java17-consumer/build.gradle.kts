plugins {
    kotlin("jvm") version "2.3.0"
    application
}

kotlin { jvmToolchain(17) }

dependencies {
    implementation("io.github.albertocavalcante:jsonrpc4k-core:${System.getenv("FIXTURE_VERSION")}")
    implementation("io.github.albertocavalcante:jsonrpc4k-transport:${System.getenv("FIXTURE_VERSION")}")
}

application { mainClass = "MainKt" }
