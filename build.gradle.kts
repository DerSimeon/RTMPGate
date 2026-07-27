plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlin.serialization)
    alias(libs.plugins.ktor)
    alias(libs.plugins.shadow)
    alias(libs.plugins.detekt)
    alias(libs.plugins.sonarqube)
    application
}

group = "lol.simeon"
version = "1.0.0"

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.ktor.server.core)
    implementation(libs.ktor.server.netty)
    implementation(libs.ktor.server.content.negotiation)
    implementation(libs.ktor.serialization.json)
    implementation(libs.ktor.server.status.pages)
    implementation(libs.ktor.server.call.logging)
    implementation(libs.ktor.server.swagger)

    implementation(libs.netty.buffer)
    implementation(libs.netty.codec)
    implementation(libs.netty.transport)
    implementation(libs.netty.handler)

    implementation(libs.lettuce.core)
    implementation(libs.caffeine)

    implementation(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.serialization.json)

    runtimeOnly(libs.logback.classic)

    testImplementation(libs.kotlin.test)
    testImplementation(libs.junit.jupiter)
}

kotlin {
    jvmToolchain(25)
}

application {
    mainClass.set("lol.simeon.rtmpgate.RTMPGateKt")
}

tasks.test {
    useJUnitPlatform()
}

// Stamp the project version into the JAR manifest so the running service can report it
// (see lol.simeon.rtmpgate.BuildInfo) instead of hard-coding "1.0.0" in several places.
tasks.withType<Jar>().configureEach {
    manifest {
        attributes["Implementation-Version"] = project.version.toString()
    }
}

detekt {
    buildUponDefaultConfig = true
    allRules = false
    parallel = true
    config.setFrom(files("$rootDir/detekt.yml"))
}


sonar {
    properties {
        property("sonar.projectKey", "DerSimeon_RTMPGate")
        property("sonar.organization","dersimeon")
        property("sonar.sources", "src/main/kotlin")
        property("sonar.tests", "src/test/kotlin")
        property("sonar.sourceEncoding", "UTF-8")
    }
}
