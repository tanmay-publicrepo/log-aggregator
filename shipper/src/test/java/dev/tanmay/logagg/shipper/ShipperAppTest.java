package dev.tanmay.logagg.shipper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.tanmay.logagg.model.LogEvent;

class ShipperAppTest {

    private static final Serializer<LogEvent> NO_OP_SERIALIZER = (topic, data) -> new byte[0];

    @TempDir
    Path dir;

    private final ByteArrayOutputStream stderr = new ByteArrayOutputStream();
    private PrintStream originalErr;

    @BeforeEach
    void captureStderr() {
        originalErr = System.err;
        System.setErr(new PrintStream(stderr, true, StandardCharsets.UTF_8));
    }

    @AfterEach
    void restoreStderr() {
        System.setErr(originalErr);
    }

    @Test
    void executeWithoutArgsReturnsUsageCode() {
        int code = ShipperApp.execute(new String[0]);

        assertThat(code).isEqualTo(ShipperApp.EXIT_USAGE);
        assertThat(stderr.toString(StandardCharsets.UTF_8)).contains("Usage");
    }

    @Test
    void executeWithMissingPathReturnsUsageCode() {
        int code = ShipperApp.execute(new String[] {dir.resolve("missing.log").toString(), "service-1"});

        assertThat(code).isEqualTo(ShipperApp.EXIT_USAGE);
        assertThat(stderr.toString(StandardCharsets.UTF_8)).contains("missing.log");
    }

    @Test
    void shipLineSkipsBlankLines() {
        MockProducer<String, LogEvent> mock = new MockProducer<>(true, new StringSerializer(), NO_OP_SERIALIZER);
        LogEventProducer producer = new LogEventProducer(mock, "logs.raw");
        LogEventMapper mapper = new LogEventMapper("test-host");

        ShipperApp.shipLine(mapper, producer, "service-1", "   ");
        assertThat(mock.history()).isEmpty();

        ShipperApp.shipLine(mapper, producer, "service-1", "hello");
        assertThat(mock.history()).hasSize(1);
        assertThat(mock.history().get(0).key()).isEqualTo("service-1");
    }

    @Test
    void shutdownSequenceRunsEveryStepEvenIfOneThrows() {
        AtomicInteger ran = new AtomicInteger();
        Runnable sequence = ShipperApp.shutdownSequence(
                ran::incrementAndGet,
                () -> { throw new IllegalStateException("boom"); },
                ran::incrementAndGet);

        assertThatCode(sequence::run).doesNotThrowAnyException();
        assertThat(ran.get()).isEqualTo(2);
    }

    @Test
    void shutdownSequenceRunsStepsInOrder() {
        List<Integer> order = new ArrayList<>();
        Runnable sequence = ShipperApp.shutdownSequence(
                () -> order.add(0),
                () -> order.add(1),
                () -> order.add(2));

        sequence.run();

        assertThat(order).containsExactly(0, 1, 2);
    }
}