# syntax=docker/dockerfile:1

FROM alpine:3.20 AS downloader
ARG CONNECTOR_TAG=3.2.0
RUN apk add --no-cache curl jq unzip
WORKDIR /dl
RUN curl -4 -fsSL --retry 5 --retry-all-errors --retry-delay 5 --connect-timeout 20 --max-time 300 \
      -o plugin.zip \
      "https://github.com/Aiven-Open/opensearch-connector-for-apache-kafka/releases/download/v${CONNECTOR_TAG}/opensearch-connector-for-apache-kafka-${CONNECTOR_TAG}.zip" \
    && mkdir -p /plugins/opensearch-sink \
    && unzip -q plugin.zip -d /plugins/opensearch-sink \
    && rm plugin.zip

FROM confluentinc/cp-kafka-connect:7.7.0
COPY --from=downloader /plugins/opensearch-sink /usr/share/confluent-hub-components/opensearch-sink