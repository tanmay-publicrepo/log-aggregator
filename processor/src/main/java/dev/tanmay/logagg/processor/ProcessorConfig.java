package dev.tanmay.logagg.processor;

import java.util.Map;

public record ProcessorConfig(
        String bootstrapServers,
        String schemaRegistryUrl,
        String applicationId,
        String inputTopic,
        String parsedTopic,
        String dlqTopic,
        int streamThreads) {

    public static ProcessorConfig from(Map<String, String> env) {
        return new ProcessorConfig(
                str(env, "KAFKA_BOOTSTRAP", "localhost:9092"),
                str(env, "SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                str(env, "STREAMS_APPLICATION_ID", "log-processor"),
                str(env, "INPUT_TOPIC", "logs.raw"),
                str(env, "PARSED_TOPIC", "logs.parsed"),
                str(env, "DLQ_TOPIC", "logs.dlq"),
                intVal(env, "STREAM_THREADS", 2));
    }

    private static String str(Map<String, String> env, String name, String defaultValue) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static int intVal(Map<String, String> env, String name, int defaultValue) {
        String raw = str(env, name, null);
        if (raw == null) {
            return defaultValue;
        }
        int value;
        try {
            value = Integer.parseInt(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a whole number, got '" + raw + "'", e);
        }
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
        return value;
    }
}