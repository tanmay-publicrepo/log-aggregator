package dev.tanmay.logagg.shipper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(15)
class TailerManagerTest {

    private static final Duration WAIT = Duration.ofSeconds(3);

    @TempDir
    Path dir;

    @Test
    void tailsExistingFilesAndPicksUpNewOnes() throws Exception {
        append(dir.resolve("service-1.log"), "one\n");
        append(dir.resolve("service-2.log"), "two\n");
        Map<String, List<String>> received = new ConcurrentHashMap<>();
        ShipperConfig config = ShipperConfig.from(
                new String[] {dir.toString()},
                Map.of("SCAN_INTERVAL_MS", "100", "POLL_INTERVAL_MS", "20"));

        try (TailerManager manager = new TailerManager(config, path -> {
            String service = TailerManager.serviceNameFor(path);
            return line -> received.computeIfAbsent(service, s -> new CopyOnWriteArrayList<>()).add(line);
        })) {
            manager.start();
            await().atMost(WAIT).until(() -> received.containsKey("service-1") && received.containsKey("service-2"));
            assertThat(received.get("service-1")).containsExactly("one");
            assertThat(received.get("service-2")).containsExactly("two");

            append(dir.resolve("service-3.log"), "three\n");
            await().atMost(WAIT).until(() -> received.containsKey("service-3"));
            assertThat(received.get("service-3")).containsExactly("three");
            assertThat(manager.activeCount()).isEqualTo(3);
        }
    }

    @Test
    void serviceNameForStripsExtension() {
        assertThat(TailerManager.serviceNameFor(Path.of("/logs/service-1.log"))).isEqualTo("service-1");
        assertThat(TailerManager.serviceNameFor(Path.of("noext"))).isEqualTo("noext");
    }

    private static void append(Path file, String text) throws IOException {
        Files.writeString(file, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}