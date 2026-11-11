# syntax=docker/dockerfile:1

FROM eclipse-temurin:21-jdk AS builder
WORKDIR /src
COPY gradlew settings.gradle.kts build.gradle.kts gradle.properties ./
COPY gradle/ gradle/
COPY shipper/build.gradle.kts shipper/
COPY model/build.gradle.kts model/
COPY processor/build.gradle.kts processor/
COPY postgres-sink-connector/build.gradle.kts postgres-sink-connector/
COPY shipper/src/ shipper/src/
COPY model/src/ model/src/
RUN chmod +x gradlew
RUN --mount=type=cache,target=/root/.gradle ./gradlew :shipper:installDist --no-daemon

FROM eclipse-temurin:21-jre
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
RUN useradd --system --create-home --uid 10001 shipper
WORKDIR /app
COPY --from=builder /src/shipper/build/install/shipper/ /app/
ENV KAFKA_BOOTSTRAP=kafka:29092 \
    SCHEMA_REGISTRY_URL=http://schema-registry:8081 \
    HEALTH_PORT=8090 \
    JAVA_OPTS="-XX:MaxRAMPercentage=75"
EXPOSE 8090
USER shipper
ENTRYPOINT ["/app/bin/shipper"]
CMD ["/logs"]