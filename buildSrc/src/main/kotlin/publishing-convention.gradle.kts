plugins {
    `maven-publish`
}

group = property("GROUP").toString()
version = property("VERSION_NAME").toString()
val githubRepo = property("GITHUB_REPO").toString()
val pomDescription = property("POM_DESCRIPTION").toString()
val githubUrl = "https://github.com/$githubRepo"

publishing {
    publications.withType<MavenPublication>().configureEach {
        pom {
            name.set(artifactId)
            description.set(pomDescription)
            url.set(githubUrl)

            licenses {
                license {
                    name.set("Apache License 2.0")
                    url.set("$githubUrl/blob/main/LICENSE")
                }
            }

            developers {
                developer {
                    id.set("albertocavalcante")
                    name.set("Alberto Cavalcante")
                    url.set("https://github.com/albertocavalcante")
                }
            }

            scm {
                url.set(githubUrl)
                connection.set("scm:git:https://github.com/$githubRepo.git")
                developerConnection.set("scm:git:ssh://git@github.com/$githubRepo.git")
            }
        }
    }

    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/$githubRepo")
            credentials {
                username = providers.gradleProperty("gpr.user").orNull ?: System.getenv("GITHUB_ACTOR") ?: ""
                password = providers.gradleProperty("gpr.key").orNull ?: System.getenv("GITHUB_TOKEN") ?: ""
            }
        }
    }
}
