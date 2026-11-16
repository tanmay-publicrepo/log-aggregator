package dev.tanmay.logagg.connector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;
import java.util.List;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

final class ParsedLogsTable {

    static final String REGION_FALLBACK = "unknown";        
    static final String ENV_FALLBACK = "unknown";        
    static final String DDL_RESOURCE = "sql/parsed_logs.sql";
    static final List<String> COLUMNS = List.of("kafka_topic", "kafka_partition", "kafka_offset", "service", "host",
            "event_time", "ingest_time", "level", "message", "trace_id", "status_code", "latency_ms", "format",
            "region", "env", "sampled", "raw");

    private ParsedLogsTable() {
    }
        static String upsertSql(String table) {
        String placeholders = String.join(", ", java.util.Collections.nCopies(COLUMNS.size(), "?"));
        return "INSERT INTO " + table + " (" + String.join(", ", COLUMNS) + ") VALUES (" + placeholders + ")"
                + " ON CONFLICT (kafka_topic, kafka_partition, kafka_offset) DO NOTHING";
    }

    static void ensure(Connection connection, String table) throws SQLException {
        String ddl = loadDdl().replace("${table}", table);
        try (Statement statement = connection.createStatement()) {
            statement.execute(ddl);
        }
    }

    private static String loadDdl() {
        try (InputStream in = ParsedLogsTable.class.getClassLoader().getResourceAsStream(DDL_RESOURCE)) {
            if (in == null) {
                throw new IllegalStateException("missing resource " + DDL_RESOURCE);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new IllegalStateException("cannot read " + DDL_RESOURCE, e);
        }
    }
        static void bind(PreparedStatement statement, SinkRecord record) throws SQLException {
        Struct value = (Struct) record.value();
        statement.setString(1, record.topic());
        statement.setInt(2, record.kafkaPartition());
        statement.setLong(3, record.kafkaOffset());
        statement.setString(4, value.getString("service"));
        statement.setString(5, value.getString("host"));
        statement.setTimestamp(6, timestamp(value.get("event_time")));
        statement.setTimestamp(7, timestamp(value.get("ingest_time")));
        statement.setString(8, value.getString("level"));
        statement.setString(9, value.getString("message"));
        statement.setString(10, value.getString("trace_id"));
        statement.setObject(11, value.getInt32("status_code"), Types.INTEGER);
        statement.setObject(12, value.getInt64("latency_ms"), Types.BIGINT);
        statement.setString(13, value.getString("format"));
         statement.setString(14, stringOr(value, "region", REGION_FALLBACK));
        statement.setString(15, stringOr(value, "env", ENV_FALLBACK));
        statement.setBoolean(16, booleanOr(value, "sampled", false));
        statement.setString(17, value.getString("raw"));
    }
        private static Timestamp timestamp(Object value) {
        return new Timestamp(((Date) value).getTime());
    }
    private static String stringOr(Struct value, String field, String fallback) {
        if (value.schema().field(field) == null) {
            return fallback;                           // record written before this field existed (schema version 1)
        }
        String text = value.getString(field);
        return text == null ? fallback : text;
    }

    private static boolean booleanOr(Struct value, String field, boolean fallback) {
        if (value.schema().field(field) == null) {
            return fallback;
        }
        Boolean flag = value.getBoolean(field);
        return flag == null ? fallback : flag;
    }
}