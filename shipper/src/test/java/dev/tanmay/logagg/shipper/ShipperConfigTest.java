package dev.tanmay.logagg.shipper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShipperConfigTest {

    @TempDir
    Path dir;

    private Path file;

    @BeforeEach
    void createFile() throws IOException {
        file = Files.createFile(dir.resolve("service-1.log"));
    }

    @Test
    void defaultsApplyWithEmptyEnv() {
        ShipperConfig config = ShipperConfig.from(args(file, "svc"), Map.of());

        assertThat(config.path()).isEqualTo(file);
        assertThat(config.serviceOverride()).isEqualTo("svc");
        assertThat(config.bootstrapServers()).isEqualTo("localhost:9092");
        assertThat(config.schemaRegistryUrl()).isEqualTo("http://localhost:8081");
        assertThat(config.topic()).isEqualTo("logs.raw");
        assertThat(config.pollIntervalMs()).isEqualTo(200L);
        assertThat(config.startAtEnd()).isFalse();
        assertThat(config.scanIntervalMs()).isEqualTo(5000L);
        assertThat(config.fileGlob()).isEqualTo("*.log");
        assertThat(config.lingerMs()).isEqualTo(20);
        assertThat(config.batchSizeBytes()).isEqualTo(65536);
        assertThat(config.compressionType()).isEqualTo("lz4");
        assertThat(config.healthPort()).isEqualTo(8090);
        assertThat(config.isDirectoryMode()).isFalse();
    }

    @Test
    void envOverridesDefault() {
        Map<String, String> env = Map.of("POLL_INTERVAL_MS", "50", "PRODUCER_COMPRESSION", "ZSTD");

        ShipperConfig config = ShipperConfig.from(args(file, "svc"), env);

        assertThat(config.pollIntervalMs()).isEqualTo(50L);
        assertThat(config.compressionType()).isEqualTo("zstd");
    }

    @Test
    void invalidNumberNamesTheVariable() {
        assertThatThrownBy(() -> ShipperConfig.from(args(file, "svc"), Map.of("POLL_INTERVAL_MS", "abc")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("POLL_INTERVAL_MS");
    }

    @Test
    void invalidBooleanIsRejected() {
        assertThatThrownBy(() -> ShipperConfig.from(args(file, "svc"), Map.of("START_AT_END", "yes")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("START_AT_END");
    }

    @Test
    void unknownCompressionIsRejected() {
        assertThatThrownBy(() -> ShipperConfig.from(args(file, "svc"), Map.of("PRODUCER_COMPRESSION", "brotli")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("PRODUCER_COMPRESSION");
    }

    @Test
    void singleFileWithoutServiceIsRejected() {
        assertThatThrownBy(() -> ShipperConfig.from(new String[] {file.toString()}, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("service name");
    }

    @Test
    void missingPathIsRejected() {
        Path missing = dir.resolve("nope.log");

        assertThatThrownBy(() -> ShipperConfig.from(new String[] {missing.toString()}, Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("nope.log");
    }

    @Test
    void directoryModeDetected() {
        ShipperConfig config = ShipperConfig.from(new String[] {dir.toString()}, Map.of());

        assertThat(config.isDirectoryMode()).isTrue();
        assertThat(config.serviceOverride()).isNull();
    }

    private static String[] args(Path path, String service) {
        return new String[] {path.toString(), service};
    }
}