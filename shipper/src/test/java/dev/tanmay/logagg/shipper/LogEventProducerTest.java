package dev.tanmay.logagg.shipper;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import dev.tanmay.logagg.model.LogEvent;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

class LogEventProducerTest {

    private static final String TOPIC = "logs.raw";
    private static final Serializer<LogEvent> NO_OP_SERIALIZER = (topic, data) -> new byte[0];

    @TempDir
    Path dir;

    @Test
    void sendRecordsToTopicWithServiceKey() {
        MockProducer<String, LogEvent> mock = new MockProducer<>(true, new StringSerializer(), NO_OP_SERIALIZER);
        LogEventProducer producer = new LogEventProducer(mock, TOPIC);
        LogEvent event = event("service-1");

        producer.send(event);

        assertThat(mock.history()).hasSize(1);
        ProducerRecord<String, LogEvent> record = mock.history().get(0);
        assertThat(record.topic()).isEqualTo(TOPIC);
        assertThat(record.key()).isEqualTo("service-1");
        assertThat(record.value()).isSameAs(event);
        assertThat(producer.sent()).isEqualTo(1L);
    }

    @Test
    void closeClosesDelegate() {
        MockProducer<String, LogEvent> mock = new MockProducer<>(true, new StringSerializer(), NO_OP_SERIALIZER);
        LogEventProducer producer = new LogEventProducer(mock, TOPIC);

        producer.close();
        assertThatCode(producer::close).doesNotThrowAnyException();

        assertThat(mock.closed()).isTrue();
    }

    @Test
    void failedSendIsLoggedNotThrown() {
        MockProducer<String, LogEvent> mock = new MockProducer<>(false, new StringSerializer(), NO_OP_SERIALIZER);
        LogEventProducer producer = new LogEventProducer(mock, TOPIC);

        assertThatCode(() -> producer.send(event("service-1"))).doesNotThrowAnyException();
        mock.errorNext(new RuntimeException("broker unavailable"));

        assertThat(producer.sent()).isZero();
    }

    @Test
    void propertiesCarryBatchingSettings() throws IOException {
        Path file = Files.createFile(dir.resolve("service-1.log"));
        Map<String, String> env = Map.of("PRODUCER_LINGER_MS", "7", "PRODUCER_COMPRESSION", "zstd");
        ShipperConfig config = ShipperConfig.from(new String[] {file.toString(), "service-1"}, env);

        Properties props = LogEventProducer.producerProperties(config);

        assertThat(props.get(ProducerConfig.LINGER_MS_CONFIG)).isEqualTo(7);
        assertThat(props.get(ProducerConfig.COMPRESSION_TYPE_CONFIG)).isEqualTo("zstd");
        assertThat(props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)).isEqualTo(KafkaAvroSerializer.class.getName());
        assertThat(props.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG)).isEqualTo(true);
    }

    private static LogEvent event(String service) {
        return LogEvent.newBuilder()
                .setService(service)
                .setHost("test-host")
                .setTimestamp(Instant.now())
                .setLevel("INFO")
                .setMessage("hello")
                .setRaw("hello")
                .build();
    }
}