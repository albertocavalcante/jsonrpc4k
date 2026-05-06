plugins {
    alias(libs.plugins.detekt)
    alias(libs.plugins.spotless)
    alias(libs.plugins.kover)
}

allprojects {
    group = property("GROUP").toString()
    version = property("VERSION_NAME").toString()
}

// Detekt configuration
detekt {
    buildUponDefaultConfig = true
    allRules = false
    config.setFrom(files("$rootDir/tools/lint/detekt.yml"))
    source.setFrom(files(subprojects.map { "${it.projectDir}/src" }))
}

dependencies {
    detektPlugins(libs.detekt.formatting)
}

// Spotless configuration
spotless {
    kotlin {
        target("**/*.kt")
        targetExclude("**/build/**", "**/buildSrc/**")
        ktlint()
    }
    kotlinGradle {
        target("**/*.gradle.kts")
        targetExclude("**/build/**", "**/buildSrc/**")
        ktlint()
    }
}

tasks.named("check") {
    dependsOn(tasks.named("detekt"))
}

// Apply Kover to all subprojects
subprojects.forEach { project ->
    project.apply(plugin = "org.jetbrains.kotlinx.kover")
}

// Merge coverage from subprojects
dependencies {
    subprojects.forEach { subproject ->
        kover(subproject)
    }
}

// Kover configuration for code coverage
kover {
    reports {
        // Configure filters to exclude test classes and generated code
        filters {
            excludes {
                classes(
                    "*Test",
                    "*Test$*",
                    "*Tests",
                    "*Tests$*",
                    "*.test.*",
                    "*.BuildConfig",
                )
            }
        }

        // Total (merged) coverage report configuration
        total {
            // HTML report for humans
            html {
                onCheck = false
                htmlDir = layout.buildDirectory.dir("reports/kover/html")
            }

            // XML report for CI integration
            xml {
                onCheck = false
                xmlFile = layout.buildDirectory.file("reports/kover/report.xml")
            }

            // Verification rules
            verify {
                rule("Merged line coverage baseline") {
                    minBound(81)
                }
            }
        }
    }
}
