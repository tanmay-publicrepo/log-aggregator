package dev.tanmay.logagg.connector;

import static dev.tanmay.logagg.connector.ParsedLogsTableTest.VALUE_SCHEMA;
import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.junit.jupiter.api.Test;

class SinkMetricsTest {

    private final Instant now = Instant.parse("2026-11-20T10:00:10Z");
    private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
    private final SinkMetrics metrics = new SinkMetrics(registry, "parsed_logs", Clock.fixed(now, ZoneOffset.UTC));
        private static SinkRecord ingestedAt(Instant ingestTime) {
        Struct value = new Struct(VALUE_SCHEMA)
                .put("service", "service-1").put("host", "mba.local")
                .put("event_time", Date.from(ingestTime.minusSeconds(1))).put("ingest_time", Date.from(ingestTime))
                .put("level", "INFO").put("message", "hello").put("format", "JSON")
                .put("region", "local").put("env", "dev").put("sampled", false).put("raw", "{}");
        return new SinkRecord("logs.parsed", 0, Schema.STRING_SCHEMA, "service-1", VALUE_SCHEMA, value, 1);
    }

    @Test
    void latencyIsRecordedPerRecordFromIngestTime() {
        metrics.recordWrite(List.of(ingestedAt(now.minusSeconds(2)), ingestedAt(now.minusSeconds(4))), 2);
        Timer timer = registry.get("sink.e2e.latency").tag("table", "parsed_logs").timer();
        assertThat(timer.count()).isEqualTo(2);
        assertThat(timer.max(TimeUnit.SECONDS)).isEqualTo(4.0);
        assertThat(timer.mean(TimeUnit.SECONDS)).isEqualTo(3.0);
    }

    @Test
    void countersFollowInsertedAndDuplicates() {
        metrics.recordWrite(List.of(ingestedAt(now), ingestedAt(now), ingestedAt(now)), 2);
        assertThat(registry.get("sink.rows.written").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("sink.rows.duplicates").counter().count()).isEqualTo(1.0);
    }

    @Test
    void retryAndFailureCountersIncrement() {
        metrics.recordRetry();
        metrics.recordRetry();
        metrics.recordFailure();
        assertThat(registry.get("sink.write.retries").counter().count()).isEqualTo(2.0);
        assertThat(registry.get("sink.write.failures").counter().count()).isEqualTo(1.0);
    }
}
