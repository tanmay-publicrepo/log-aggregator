package dev.tanmay.logagg.connector;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresSinkTaskTest {

    private static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("ParsedLogEvent")
            .field("service", Schema.STRING_SCHEMA)
            .field("level", Schema.STRING_SCHEMA)
            .field("message", Schema.STRING_SCHEMA)
            .build();

    private PostgresSinkTask task;
        @BeforeEach
    void startTask() {
        task = new PostgresSinkTask();
        task.start(Map.of(
                PostgresSinkConfig.CONNECTION_URL, "jdbc:postgresql://localhost:5432/logs",
                PostgresSinkConfig.CONNECTION_USER, "logs",
                PostgresSinkConfig.CONNECTION_PASSWORD, "logs"));
    }

    private static SinkRecord record(int partition, long offset, String service) {
        Struct value = new Struct(VALUE_SCHEMA).put("service", service).put("level", "INFO").put("message", "hello");
        return new SinkRecord("logs.parsed", partition, Schema.STRING_SCHEMA, service, VALUE_SCHEMA, value, offset);
    }

    @Test
    void putBuffersAndCounts() {
        task.put(List.of(record(0, 10, "service-1"), record(1, 3, "service-2")));
        task.put(List.of(record(0, 11, "service-1")));
        assertThat(task.received()).isEqualTo(3);
        assertThat(task.buffered()).isEqualTo(3);
    }

    @Test
    void flushClearsBuffer() {
        task.put(List.of(record(0, 10, "service-1")));
        task.flush(Map.of());
        assertThat(task.buffered()).isZero();
        assertThat(task.received()).isEqualTo(1);
    }

    @Test
    void emptyPutIsNoop() {
        task.put(List.of());
        assertThat(task.received()).isZero();
        assertThat(task.buffered()).isZero();
    }
}