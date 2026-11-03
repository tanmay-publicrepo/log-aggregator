/** About this class - Manages multiple FileTailer instances for a given directory and file glob pattern. It scans the directory at regular intervals to detect new files matching the glob pattern and starts a new FileTailer for each detected file. The TailerManager uses a ScheduledExecutorService to perform the scanning and an ExecutorService to run the tailers concurrently. It also provides methods to start, stop, and monitor the active tailers. */

package dev.tanmay.logagg.shipper;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TailerManager implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(TailerManager.class);
    private static final long SHUTDOWN_WAIT_SECONDS = 10;

    private final Path dir;
    private final String glob;
    private final long pollIntervalMs;
    private final boolean startAtEnd;
    private final long scanIntervalMs;
    private final Function<Path, Consumer<String>> consumerFactory;
    private final Map<Path, FileTailer> tailers = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scanner;
    private final ExecutorService workers;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public TailerManager(ShipperConfig config, Function<Path, Consumer<String>> consumerFactory) {
        this.dir = config.path();
        this.glob = config.fileGlob();
        this.pollIntervalMs = config.pollIntervalMs();
        this.startAtEnd = config.startAtEnd();
        this.scanIntervalMs = config.scanIntervalMs();
        this.consumerFactory = consumerFactory;
        this.scanner = Executors.newSingleThreadScheduledExecutor(
                Thread.ofPlatform().name("tailer-scanner").factory());
        this.workers = Executors.newCachedThreadPool(
                Thread.ofPlatform().name("tailer-worker-", 0).factory());
    }

    public void start() {
        log.info("Watching {} for {} every {} ms", dir, glob, scanIntervalMs);
        scanner.scheduleWithFixedDelay(this::scan, 0, scanIntervalMs, TimeUnit.MILLISECONDS);
    }

    public int activeCount() {
        return tailers.size();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        scanner.shutdownNow();
        int count = tailers.size();
        tailers.values().forEach(FileTailer::stop);
        workers.shutdown();
        try {
            if (!workers.awaitTermination(SHUTDOWN_WAIT_SECONDS, TimeUnit.SECONDS)) {
                log.warn("Some tailers did not stop within {} s", SHUTDOWN_WAIT_SECONDS);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        log.info("Stopped {} tailers", count);
    }

    private void scan() {
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(dir, glob)) {
            for (Path file : stream) {
                if (Files.isRegularFile(file) && !tailers.containsKey(file)) {
                    startTailer(file);
                }
            }
        } catch (Exception e) {
            log.error("Scan of {} failed", dir, e);
        }
    }

    private void startTailer(Path file) {
        FileTailer tailer = new FileTailer(file, consumerFactory.apply(file), pollIntervalMs, startAtEnd);
        tailers.put(file, tailer);
        workers.submit(() -> {
            String originalName = Thread.currentThread().getName();
            Thread.currentThread().setName("tailer-" + file.getFileName());
            try {
                tailer.run();
            } catch (Throwable t) {
                log.error("Tailer for {} died", file, t);
                tailers.remove(file);
            } finally {
                Thread.currentThread().setName(originalName);
            }
        });
        log.info("Started tailer for {} as service {}", file, serviceNameFor(file));
    }

    static String serviceNameFor(Path file) {
        String name = file.getFileName().toString();
        int dot = name.lastIndexOf('.');
        return dot > 0 ? name.substring(0, dot) : name;
    }
}