package dev.tanmay.logagg.connector;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.time.Clock;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;

final class SinkMetrics {

    static final String INGEST_TIME_FIELD = "ingest_time";

    private final Clock clock;
    private final Timer latency;
    private final Counter rowsWritten;
    private final Counter duplicates;
    private final Counter retries;
    private final Counter failures;

        SinkMetrics(MeterRegistry registry, String table) {
        this(registry, table, Clock.systemUTC());
    }

    SinkMetrics(MeterRegistry registry, String table, Clock clock) {
        this.clock = clock;
        latency = Timer.builder("sink.e2e.latency")
                .description("time from the shipper reading a line (ingest_time) to its commit in Postgres")
                .tag("table", table)
                .publishPercentileHistogram()
                .minimumExpectedValue(Duration.ofMillis(10))
                .maximumExpectedValue(Duration.ofMinutes(5))
                .register(registry);
        rowsWritten = Counter.builder("sink.rows.written").tag("table", table).register(registry);
        duplicates = Counter.builder("sink.rows.duplicates").tag("table", table).register(registry);
        retries = Counter.builder("sink.write.retries").tag("table", table).register(registry);
        failures = Counter.builder("sink.write.failures").tag("table", table).register(registry);
    }
        void recordWrite(Collection<SinkRecord> batch, int inserted) {
        var now = clock.instant();
        for (SinkRecord record : batch) {
            Struct value = (Struct) record.value();
            Field field = value.schema().field(INGEST_TIME_FIELD);
            if (field != null && value.get(field) instanceof Date ingested) {
                Duration lag = Duration.between(ingested.toInstant(), now);
                if (!lag.isNegative()) {
                    latency.record(lag);
                }
            }
        }
        rowsWritten.increment(inserted);
        duplicates.increment(batch.size() - inserted);
    }

    void recordRetry() { retries.increment(); }
    void recordFailure() { failures.increment(); }
}