package dev.tanmay.logagg.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;


class ParsedLogsTableTest {

    static final Schema VALUE_SCHEMA = SchemaBuilder.struct().name("ParsedLogEvent")
            .field("service", Schema.STRING_SCHEMA).field("host", Schema.STRING_SCHEMA)
            .field("event_time", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .field("ingest_time", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .field("level", Schema.STRING_SCHEMA).field("message", Schema.STRING_SCHEMA)
            .field("trace_id", Schema.OPTIONAL_STRING_SCHEMA).field("status_code", Schema.OPTIONAL_INT32_SCHEMA)
            .field("latency_ms", Schema.OPTIONAL_INT64_SCHEMA).field("format", Schema.STRING_SCHEMA)
            .field("region", Schema.STRING_SCHEMA).field("env", Schema.STRING_SCHEMA)
            .field("sampled", Schema.BOOLEAN_SCHEMA).field("raw", Schema.STRING_SCHEMA).build();
        static SinkRecord record(int partition, long offset, String service, Integer status, String traceId) {
        Struct value = new Struct(VALUE_SCHEMA)
                .put("service", service).put("host", "mba.local")
                .put("event_time", new Date(1_700_000_000_000L)).put("ingest_time", new Date(1_700_000_000_500L))
                .put("level", "INFO").put("message", "hello").put("trace_id", traceId)
                .put("status_code", status).put("latency_ms", status == null ? null : 37L)
                .put("format", "JSON").put("region", "local").put("env", "dev")
                .put("sampled", false).put("raw", "{...}");
        return new SinkRecord("logs.parsed", partition, Schema.STRING_SCHEMA, service, VALUE_SCHEMA, value, offset);
    }

    static final Schema OLD_VALUE_SCHEMA = SchemaBuilder.struct().name("ParsedLogEvent")
            .field("service", Schema.STRING_SCHEMA).field("host", Schema.STRING_SCHEMA)
            .field("event_time", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .field("ingest_time", org.apache.kafka.connect.data.Timestamp.SCHEMA)
            .field("level", Schema.STRING_SCHEMA).field("message", Schema.STRING_SCHEMA)
            .field("trace_id", Schema.OPTIONAL_STRING_SCHEMA).field("status_code", Schema.OPTIONAL_INT32_SCHEMA)
            .field("latency_ms", Schema.OPTIONAL_INT64_SCHEMA).field("format", Schema.STRING_SCHEMA)
            .field("raw", Schema.STRING_SCHEMA).build();

      @Test
    void bindFallsBackForFieldsMissingFromOlderSchemas() throws Exception {
        Struct old = new Struct(OLD_VALUE_SCHEMA)
                .put("service", "service-1").put("host", "mba.local")
                .put("event_time", new Date(1_700_000_000_000L)).put("ingest_time", new Date(1_700_000_000_500L))
                .put("level", "INFO").put("message", "hello").put("format", "TEXT").put("raw", "...");
        SinkRecord record = new SinkRecord("logs.parsed", 0, Schema.STRING_SCHEMA, "service-1", OLD_VALUE_SCHEMA, old, 1);
        PreparedStatement statement = mock(PreparedStatement.class);
        ParsedLogsTable.bind(statement, record);
        verify(statement).setString(14, ParsedLogsTable.REGION_FALLBACK);
        verify(statement).setString(15, ParsedLogsTable.ENV_FALLBACK);
        verify(statement).setBoolean(16, false);
    }

    @Test
    void upsertSqlListsEveryColumnOnceAndTargetsTheKafkaKey() {
        String sql = ParsedLogsTable.upsertSql("parsed_logs");
        assertThat(sql).startsWith("INSERT INTO parsed_logs (kafka_topic, kafka_partition, kafka_offset, service");
        assertThat(sql).contains("ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING");
        assertThat(sql.chars().filter(c -> c == '?').count()).isEqualTo(17);
    }

    @Test
    void bindMapsCoordinatesAndFieldsToPlaceholders() throws Exception {
        PreparedStatement statement = mock(PreparedStatement.class);
        ParsedLogsTable.bind(statement, record(4, 2317, "service-1", null, null));
        verify(statement).setString(1, "logs.parsed");
        verify(statement).setInt(2, 4);
        verify(statement).setLong(3, 2317L);
        verify(statement).setString(4, "service-1");
        verify(statement).setTimestamp(6, new Timestamp(1_700_000_000_000L));
        verify(statement).setString(10, null);
        verify(statement).setObject(11, null, Types.INTEGER);
        verify(statement).setBoolean(16, false);
    }
}