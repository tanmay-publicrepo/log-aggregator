package dev.tanmay.logagg.shipper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(10)
class FileTailerTest {

    private static final Duration WAIT = Duration.ofSeconds(2);

    @TempDir
    Path dir;

    private record RunningTailer(FileTailer tailer, Thread thread, List<String> lines) { }

    @Test
    void deliversExistingLines() throws Exception {
        Path file = dir.resolve("a.log");
        append(file, "one\ntwo\nthree\n");

        RunningTailer running = start(file, false);
        await().atMost(WAIT).until(() -> running.lines().size() == 3);

        assertThat(running.lines()).containsExactly("one", "two", "three");
        stopAndJoin(running);
    }

    @Test
    void deliversAppendedLines() throws Exception {
        Path file = dir.resolve("a.log");
        Files.createFile(file);
        RunningTailer running = start(file, false);

        append(file, "later\n");

        await().atMost(WAIT).until(() -> running.lines().contains("later"));
        stopAndJoin(running);
    }

    @Test
    void holdsPartialLineUntilNewline() throws Exception {
        Path file = dir.resolve("a.log");
        Files.createFile(file);
        RunningTailer running = start(file, false);

        append(file, "abc");
        await().during(Duration.ofMillis(300)).atMost(WAIT).until(() -> running.lines().isEmpty());

        append(file, "\n");
        await().atMost(WAIT).until(() -> running.lines().size() == 1);
        assertThat(running.lines()).containsExactly("abc");
        stopAndJoin(running);
    }

    @Test
    void startAtEndSkipsExistingLines() throws Exception {
        Path file = dir.resolve("a.log");
        
        append(file, "old\n");
        RunningTailer running = start(file, true);

        await().atMost(WAIT).until(() -> running.tailer().opens() == 1);
        append(file, "new\n");

        await().atMost(WAIT).untilAsserted(() -> assertThat(running.lines()).containsExactly("new"));
        stopAndJoin(running);
    }

    @Test
    void truncationResetsPosition() throws Exception {
        Path file = dir.resolve("a.log");
        append(file, "one\ntwo\nthree\n");
        RunningTailer running = start(file, false);
        await().atMost(WAIT).until(() -> running.lines().size() == 3);

        Files.write(file, new byte[0], StandardOpenOption.TRUNCATE_EXISTING);
        await().atMost(WAIT).until(() -> running.tailer().truncations() == 1);
        append(file, "after\n");

        await().atMost(WAIT).until(() -> running.lines().size() == 4);
        assertThat(running.lines().get(3)).isEqualTo("after");
        stopAndJoin(running);
    }

    @Test
    void rotationFollowsNewFile() throws Exception {
        Path file = dir.resolve("a.log");
        append(file, "before\n");
        RunningTailer running = start(file, false);
        await().atMost(WAIT).until(() -> running.lines().size() == 1);

        Files.move(file, dir.resolve("a.log.1"), StandardCopyOption.ATOMIC_MOVE);
        append(file, "after\n");

        await().atMost(WAIT).until(() -> running.lines().size() == 2);
        assertThat(running.lines()).containsExactly("before", "after");
        assertThat(running.tailer().rotations()).isEqualTo(1);
        stopAndJoin(running);
    }

    @Test
    void decodesMultiByteUtf8() throws Exception {
        Path file = dir.resolve("a.log");
        String line = "\u0928\u092E\u0938\u094D\u0924\u0947 \uD83D\uDE80";
        append(file, line + "\n");

        RunningTailer running = start(file, false);
        await().atMost(WAIT).until(() -> running.lines().size() == 1);

        assertThat(running.lines()).containsExactly(line);
        stopAndJoin(running);
    }

    @Test
    void stopTerminatesThread() throws Exception {
        Path file = dir.resolve("a.log");
        Files.createFile(file);
        RunningTailer running = start(file, false);

        stopAndJoin(running);

        assertThat(running.thread().isAlive()).isFalse();
    }

    @Test
    void callbackExceptionDoesNotStopTailer() throws Exception {
        Path file = dir.resolve("a.log");
        append(file, "bad\ngood\n");
        List<String> lines = new CopyOnWriteArrayList<>();
        AtomicInteger calls = new AtomicInteger();
        Consumer<String> consumer = line -> {
            if (calls.incrementAndGet() == 1) {
                throw new IllegalStateException("boom");
            }
            lines.add(line);
        };
        FileTailer tailer = new FileTailer(file, consumer, 20, false);
        Thread thread = runOnThread(tailer);

        await().atMost(WAIT).until(() -> lines.contains("good"));

        assertThat(tailer.callbackFailures()).isEqualTo(1);
        tailer.stop();
        thread.join(WAIT.toMillis());
    }

    private RunningTailer start(Path file, boolean startAtEnd) {
        List<String> lines = new CopyOnWriteArrayList<>();
        FileTailer tailer = new FileTailer(file, lines::add, 20, startAtEnd);
        return new RunningTailer(tailer, runOnThread(tailer), lines);
    }

    private static Thread runOnThread(FileTailer tailer) {
        Thread thread = new Thread(() -> {
            try {
                tailer.run();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "tailer-test");
        thread.start();
        return thread;
    }

    private static void stopAndJoin(RunningTailer running) throws InterruptedException {
        running.tailer().stop();
        running.thread().join(WAIT.toMillis());
    }

    private static void append(Path file, String text) throws IOException {
        Files.writeString(file, text, StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }
}