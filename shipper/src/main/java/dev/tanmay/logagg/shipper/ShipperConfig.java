/** About this class - Represents the configuration for the log shipper application. It holds all the necessary parameters for initializing and running the shipper, including file paths, service names, and Kafka producer settings. */

package dev.tanmay.logagg.shipper;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public record ShipperConfig(
        Path path,
        String serviceOverride,
        String bootstrapServers,
        String schemaRegistryUrl,
        String topic,
        long pollIntervalMs,
        boolean startAtEnd,
        long scanIntervalMs,
        String fileGlob,
        int lingerMs,
        int batchSizeBytes,
        String compressionType,
        int healthPort) {

    private static final Set<String> COMPRESSION_TYPES = Set.of("none", "gzip", "snappy", "lz4", "zstd");

    public static ShipperConfig from(String[] args, Map<String, String> env) {
        if (args.length < 1 || args.length > 2) {
            throw new IllegalArgumentException(
                    "expected <file-or-directory> [service-name], got " + args.length + " arguments");
        }

        Path path = Path.of(args[0]);
        if (!Files.exists(path)) {
            throw new IllegalArgumentException("path does not exist: " + path);
        }

        String serviceOverride = args.length == 2 ? args[1] : null;
        if (serviceOverride != null && serviceOverride.isBlank()) {
            throw new IllegalArgumentException("service name must not be blank");
        }
        if (Files.isRegularFile(path) && serviceOverride == null) {
            throw new IllegalArgumentException("service name is required when tailing a single file");
        }
        if (Files.isDirectory(path) && serviceOverride != null) {
            throw new IllegalArgumentException(
                    "service names come from file names in directory mode; drop the service argument");
        }

        String bootstrapServers = str(env, "KAFKA_BOOTSTRAP", "localhost:9092");
        String schemaRegistryUrl = str(env, "SCHEMA_REGISTRY_URL", "http://localhost:8081");
        String topic = str(env, "LOGS_TOPIC", "logs.raw");
        long pollIntervalMs = longVal(env, "POLL_INTERVAL_MS", 200);
        boolean startAtEnd = boolVal(env, "START_AT_END", false);
        long scanIntervalMs = longVal(env, "SCAN_INTERVAL_MS", 5000);
        String fileGlob = str(env, "FILE_GLOB", "*.log");
        int lingerMs = intVal(env, "PRODUCER_LINGER_MS", 20);
        int batchSizeBytes = intVal(env, "PRODUCER_BATCH_SIZE", 65536);
        String compressionType = str(env, "PRODUCER_COMPRESSION", "lz4").toLowerCase(Locale.ROOT);
        int healthPort = intVal(env, "HEALTH_PORT", 8090);

        if (!COMPRESSION_TYPES.contains(compressionType)) {
            throw new IllegalArgumentException(
                    "PRODUCER_COMPRESSION must be one of " + COMPRESSION_TYPES + ", got '" + compressionType + "'");
        }

        return new ShipperConfig(
                path,
                serviceOverride,
                bootstrapServers,
                schemaRegistryUrl,
                topic,
                pollIntervalMs,
                startAtEnd,
                scanIntervalMs,
                fileGlob,
                lingerMs,
                batchSizeBytes,
                compressionType,
                healthPort);
    }

    public boolean isDirectoryMode() {
        return Files.isDirectory(path);
    }

    private static String str(Map<String, String> env, String name, String defaultValue) {
        String value = env.get(name);
        if (value == null || value.isBlank()) {
            return defaultValue;
        }
        return value.trim();
    }

    private static long longVal(Map<String, String> env, String name, long defaultValue) {
        String raw = str(env, name, null);
        if (raw == null) {
            return defaultValue;
        }
        long value;
        try {
            value = Long.parseLong(raw);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(name + " must be a whole number, got '" + raw + "'", e);
        }
        if (value <= 0) {
            throw new IllegalArgumentException(name + " must be positive, got " + value);
        }
        return value;
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

    private static boolean boolVal(Map<String, String> env, String name, boolean defaultValue) {
        String raw = str(env, name, null);
        if (raw == null) {
            return defaultValue;
        }
        return switch (raw.toLowerCase(Locale.ROOT)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(name + " must be true or false, got '" + raw + "'");
        };
    }
}