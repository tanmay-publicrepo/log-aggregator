/**About this class - The main entry point for the log shipper application. It parses command-line arguments, initializes the necessary components (such as LogEventProducer, HealthServer, and FileTailer or TailerManager), and manages the application's lifecycle. It handles both file mode (tailing a single file) and directory mode (watching a directory for log files). The class also sets up a shutdown hook to gracefully stop the application and its components when a termination signal is received. */

package dev.tanmay.logagg.shipper;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShipperApp {

    private static final Logger log = LoggerFactory.getLogger(ShipperApp.class);

    static final int EXIT_OK = 0;
    static final int EXIT_FAILURE = 1;
    static final int EXIT_USAGE = 2;

    private static final String USAGE = "Usage: shipper <file-or-directory> [service-name]";
    private static final Duration STOP_TIMEOUT = Duration.ofSeconds(5);

    public static void main(String[] args) {
        System.exit(execute(args));
    }

    static int execute(String[] args) {
        ShipperConfig config;
        try {
            config = ShipperConfig.from(args, System.getenv());
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            System.err.println(USAGE);
            return EXIT_USAGE;
        }

        log.info("Starting shipper with {}", config);
        LogEventMapper mapper = LogEventMapper.forLocalHost();
        return run(config, mapper);
    }

    static int run(ShipperConfig config, LogEventMapper mapper) {
        AtomicBoolean ready = new AtomicBoolean(false);

        try (LogEventProducer producer = new LogEventProducer(config);
             HealthServer health = new HealthServer(config.healthPort(), ready::get)) {

            health.start();

            if (config.isDirectoryMode()) {
                return runDirectoryMode(config, mapper, producer, health, ready);
            }
            return runFileMode(config, mapper, producer, health, ready);

        } catch (IOException e) {
            log.error("I/O failure: {}", e.getMessage(), e);
            return EXIT_FAILURE;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while running, shutting down");
            return EXIT_FAILURE;
        } catch (RuntimeException e) {
            log.error("Fatal error", e);
            return EXIT_FAILURE;
        }
    }

    private static int runFileMode(ShipperConfig config, LogEventMapper mapper, LogEventProducer producer,
                                   HealthServer health, AtomicBoolean ready) throws IOException {
        String service = config.serviceOverride();
        Consumer<String> consumer = line -> shipLine(mapper, producer, service, line);
        FileTailer tailer = new FileTailer(config.path(), consumer, config.pollIntervalMs(), config.startAtEnd());
        CountDownLatch tailerDone = new CountDownLatch(1);

        registerShutdownHook(shutdownSequence(
                () -> ready.set(false),
                tailer::stop,
                () -> awaitQuietly(tailerDone, STOP_TIMEOUT),
                health::close,
                producer::close));

        ready.set(true);
        log.info("Tailing {} as service {}", config.path(), service);
        try {
            tailer.run();
        } finally {
            tailerDone.countDown();
        }
        return EXIT_OK;
    }

    private static int runDirectoryMode(ShipperConfig config, LogEventMapper mapper, LogEventProducer producer,
                                        HealthServer health, AtomicBoolean ready) throws InterruptedException {
        Function<Path, Consumer<String>> consumerFactory = path -> {
            String service = TailerManager.serviceNameFor(path);
            return line -> shipLine(mapper, producer, service, line);
        };

        try (TailerManager manager = new TailerManager(config, consumerFactory)) {
            CountDownLatch stopped = new CountDownLatch(1);

            registerShutdownHook(shutdownSequence(
                    () -> ready.set(false),
                    manager::close,
                    health::close,
                    producer::close,
                    stopped::countDown));

            manager.start();
            ready.set(true);
            log.info("Watching {} for {}", config.path(), config.fileGlob());
            stopped.await();
            return EXIT_OK;
        }
    }

    static void shipLine(LogEventMapper mapper, LogEventProducer producer, String service, String line) {
        if (line.isBlank()) {
            return;
        }
        producer.send(mapper.toEvent(line, service));
    }

    static Runnable shutdownSequence(Runnable... steps) {
        return () -> {
            log.info("Shutdown signal received");
            for (int i = 0; i < steps.length; i++) {
                try {
                    steps[i].run();
                } catch (Exception e) {
                    log.error("Shutdown step {} failed", i, e);
                }
            }
            log.info("Shutdown complete");
        };
    }

    private static void registerShutdownHook(Runnable sequence) {
        Runtime.getRuntime().addShutdownHook(new Thread(sequence, "shipper-shutdown"));
    }

    private static void awaitQuietly(CountDownLatch latch, Duration timeout) {
        try {
            if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
                log.warn("Tailer did not stop within {}", timeout);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}