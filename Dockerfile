FROM eclipse-temurin:25-jre-alpine

WORKDIR /app

COPY build/libs/*-all.jar /app/rtmpgate.jar

EXPOSE 8080 1935

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/rtmpgate.jar"]
