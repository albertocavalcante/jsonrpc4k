plugins {
    `kotlin-dsl`
}

repositories {
    mavenCentral()
    gradlePluginPortal()
}

dependencies {
    implementation("org.jetbrains.kotlin:kotlin-gradle-plugin:2.4.0")
    implementation("org.jetbrains.kotlin:kotlin-serialization:2.4.0")
}
