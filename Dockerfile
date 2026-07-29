# syntax=docker/dockerfile:1

# ---- Build stage: compile the shadow (fat) jar from source ----
FROM eclipse-temurin:25-jdk-alpine AS build
WORKDIR /build

# Warm the Gradle/dependency cache on the wrapper + build scripts before copying sources,
# so source-only changes don't re-resolve dependencies.
COPY gradlew ./
COPY gradle ./gradle
COPY settings.gradle.kts build.gradle.kts gradle.properties ./
RUN chmod +x ./gradlew && ./gradlew --no-daemon --version

COPY src ./src
COPY detekt.yml ./
RUN ./gradlew --no-daemon shadowJar

# ---- Runtime stage: minimal JRE, non-root ----
FROM eclipse-temurin:25-jre-alpine AS runtime

# wget (busybox) is used by the container HEALTHCHECK.
RUN addgroup -S -g 10001 rtmpgate && adduser -S -u 10001 -G rtmpgate -h /app rtmpgate

WORKDIR /app
COPY --from=build /build/build/libs/*-all.jar /app/rtmpgate.jar

USER 10001

EXPOSE 8080 1935

# Liveness only — does not depend on Redis (see /livez). Kubernetes probes are configured
# separately in the Helm chart; this HEALTHCHECK is for plain Docker / compose runs.
HEALTHCHECK --interval=30s --timeout=3s --start-period=20s --retries=3 \
    CMD wget -q -O - http://127.0.0.1:8080/livez >/dev/null 2>&1 || exit 1

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/rtmpgate.jar"]
