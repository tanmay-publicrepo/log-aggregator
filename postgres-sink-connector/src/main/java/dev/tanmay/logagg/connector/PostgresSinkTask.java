package dev.tanmay.logagg.connector;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.kafka.connect.errors.ConnectException;


public class PostgresSinkTask extends SinkTask {

    static final long MAX_BACKOFF_MS = 30_000L;
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSinkTask.class);
        private final Function<PostgresSinkConfig, PostgresWriter> writerFactory;
    private PostgresSinkConfig config;
    private PostgresWriter writer;
    private long received;
    private long written;
    private int attempt;
    private int remainingRetries;
    private SinkMetrics metrics;

    public PostgresSinkTask() {
        this(PostgresWriter::new);
    }

    PostgresSinkTask(Function<PostgresSinkConfig, PostgresWriter> writerFactory) {
        this.writerFactory = writerFactory;
    }

    @Override
    public String version() {
        return PostgresSinkConnector.VERSION;
    }
        @Override
    public void start(Map<String, String> props) {
        config = new PostgresSinkConfig(props);
        writer = writerFactory.apply(config);
        MeterRegistry registry = config.metricsEnabled()? MetricsEndpoint.shared(config.metricsPort()).registry()
        : new SimpleMeterRegistry();
        metrics = new SinkMetrics(registry, config.tableName());
        remainingRetries = config.maxRetries();
        LOG.info("PostgresSinkTask started: table={} batchSize={} maxRetries={}",
                config.tableName(), config.batchSize(), config.maxRetries());
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        received += records.size();
        List<SinkRecord> batch = new ArrayList<>(records.size());
        for (SinkRecord record : records) {
            if (record.value() == null) {
                LOG.debug("skipping tombstone {}-{}@{}", record.topic(), record.kafkaPartition(), record.kafkaOffset());
                continue;
            }
            batch.add(record);
        }
        if (!batch.isEmpty()) {
            writeWithRetry(batch);
        }
    }
    private void writeWithRetry(List<SinkRecord> batch) {
        try {
            
            int inserted = writer.write(batch);
            written += inserted;
            metrics.recordWrite(batch, inserted);
            attempt = 0;
            remainingRetries = config.maxRetries();
            LOG.debug("wrote {} rows ({} duplicates skipped), total written {}", inserted, batch.size() - inserted, written);
        } catch (SQLException e) {
            if (PostgresWriter.isRetriable(e) && remainingRetries > 0) {
                long backoff = backoffMs();
                attempt++;
                remainingRetries--;
                context.timeout(backoff);
                LOG.warn("write failed ({}), retrying in {} ms, {} retries left", e.getMessage(), backoff, remainingRetries);
                metrics.recordRetry();
                throw new RetriableException(e);
            }
            metrics.recordFailure();
            throw new ConnectException("Giving up after " + attempt + " retries: " + e.getMessage(), e);
        }
    }

    private long backoffMs() {
        return Math.min(config.retryBackoffMs() * (1L << attempt), MAX_BACKOFF_MS);
    }
        @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        LOG.debug("flush: {} partitions, nothing buffered (writes are synchronous)", currentOffsets.size());
    }

    @Override
    public void stop() {
        if (writer != null) {
            writer.close();
        }
        LOG.info("PostgresSinkTask stopped: received={} written={}", received, written);
    }

    long received() {
        return received;
    }

    long written() {
        return written;
    }

    int attempt() {
        return attempt;
    }
}