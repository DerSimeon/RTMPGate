package lol.simeon.rtmpgate

/**
 * Single source of truth for the running service version.
 *
 * The value comes from the JAR manifest `Implementation-Version` attribute, which the Gradle
 * build stamps from `project.version` (see build.gradle.kts). When running from the classpath
 * (e.g. `gradlew run`) the manifest is absent, so it falls back to "dev".
 */
object BuildInfo {
    val version: String = BuildInfo::class.java.`package`?.implementationVersion ?: "dev"
}
