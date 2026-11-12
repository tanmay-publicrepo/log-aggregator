package dev.tanmay.logagg.processor;

import java.util.Map;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.Set;

public record ProcessorConfig(
        String bootstrapServers,
        String schemaRegistryUrl,
        String applicationId,
        String inputTopic,
        String parsedTopic,
        String dlqTopic,
        int streamThreads,
        String region,
        String env,
        Set<String> noisyServices,
        String sampleService,
        double sampleRate) {

    public static ProcessorConfig from(Map<String, String> env) {
        return new ProcessorConfig(
                str(env, "KAFKA_BOOTSTRAP", "localhost:9092"),
                str(env, "SCHEMA_REGISTRY_URL", "http://localhost:8081"),
                str(env, "STREAMS_APPLICATION_ID", "log-processor"),
                str(env, "INPUT_TOPIC", "logs.raw"),
                str(env, "PARSED_TOPIC", "logs.parsed"),
                str(env, "DLQ_TOPIC", "logs.dlq"),
                intVal(env, "STREAM_THREADS", 2),
                str(env, "ENRICH_REGION", "local"),
                str(env, "ENRICH_ENV", "dev"),
                setVal(env, "NOISY_SERVICES", "service-3"),
                str(env, "SAMPLE_SERVICE", "service-4"),
                doubleVal(env, "SAMPLE_RATE", 0.1));
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

    private static Set<String> setVal(Map<String, String> env, String name, String defaultValue) {
        String raw = str(env, name, defaultValue);
        return Arrays.stream(raw.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    private static double doubleVal(Map<String, String> env, String name, double defaultValue) {
        String raw = str(env, name, null);
        if (raw == null) {
            return defaultValue;
        }
        double value;
        try {
            value = Double.parseDouble(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a number, got '" + raw + "'", e);
        }
        if (value < 0.0 || value > 1.0) {
            throw new IllegalArgumentException(name + " must be between 0 and 1, got " + value);
        }
        return value;
    }
}

