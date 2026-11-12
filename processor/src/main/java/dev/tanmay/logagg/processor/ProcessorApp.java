package dev.tanmay.logagg.processor;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessorApp {

    private static final Logger log = LoggerFactory.getLogger(ProcessorApp.class);

    static final int EXIT_OK = 0;
    static final int EXIT_FAILURE = 1;
    static final int EXIT_USAGE = 2;

    public static void main(String[] args) {
        System.exit(execute(System.getenv()));
    }

    static int execute(Map<String, String> env) {
        ProcessorConfig config;
        try {
            config = ProcessorConfig.from(env);
        } catch (IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return EXIT_USAGE;
        }
        log.info("Starting processor with {}", config);

        Topology topology = new TopologyBuilder(config, new LineParser()).build();
        log.info("Topology:\n{}", topology.describe());
        return run(config, topology);
    }

    static int run(ProcessorConfig config, Topology topology) {
        CountDownLatch stopped = new CountDownLatch(1);

        try (KafkaStreams streams = new KafkaStreams(topology, streamsProperties(config))) {
            streams.setStateListener((newState, oldState) ->
                    log.info("Streams state {} -> {}", oldState, newState));
            streams.setUncaughtExceptionHandler(e -> {
                log.error("Stream thread died; replacing it", e);
                return StreamThreadExceptionResponse.REPLACE_THREAD;
            });
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutdown signal received");
                streams.close(Duration.ofSeconds(10));
                stopped.countDown();
            }, "processor-shutdown"));

            streams.start();
            stopped.await();
            return EXIT_OK;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted, shutting down");
            return EXIT_FAILURE;
        } catch (RuntimeException e) {
            log.error("Fatal error", e);
            return EXIT_FAILURE;
        }
    }

    static Properties streamsProperties(ProcessorConfig config) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, config.applicationId());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, config.streamThreads());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }
}