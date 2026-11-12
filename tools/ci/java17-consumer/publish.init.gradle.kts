import org.gradle.api.publish.PublishingExtension
import org.gradle.api.tasks.testing.Test

allprojects {
    tasks.withType<Test>().configureEach {
        doFirst {
            val version =
                javaLauncher
                    .get()
                    .metadata.languageVersion
                    .asInt()
            check(version == 17) { "JVM tests must run on Java 17, got $version" }
            println("JVM tests use Java $version")
        }
    }
    plugins.withId("maven-publish") {
        extensions.configure<PublishingExtension> {
            repositories.maven {
                name = "Fixture"
                url = uri(requireNotNull(System.getenv("FIXTURE_REPOSITORY")))
            }
        }
    }
}
