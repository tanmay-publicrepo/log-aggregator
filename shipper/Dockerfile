# syntax=docker/dockerfile:1

FROM eclipse-temurin:21-jdk AS builder
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle/ gradle/
COPY model/build.gradle.kts model/
COPY shipper/build.gradle.kts shipper/
COPY processor/build.gradle.kts processor/
COPY postgres-sink-connector/build.gradle.kts postgres-sink-connector/
COPY model/src/ model/src/
COPY processor/src/ processor/src/
RUN chmod +x gradlew
RUN --mount=type=cache,target=/root/.gradle ./gradlew :processor:installDist --no-daemon

FROM eclipse-temurin:21-jre
RUN useradd --system --create-home --uid 10002 processor
WORKDIR /app
COPY --from=builder /src/processor/build/install/processor/ /app/
ENV KAFKA_BOOTSTRAP=kafka:29092 \
    SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    JAVA_OPTS="-XX:MaxRAMPercentage=75"
USER processor
ENTRYPOINT ["/app/bin/processor"]