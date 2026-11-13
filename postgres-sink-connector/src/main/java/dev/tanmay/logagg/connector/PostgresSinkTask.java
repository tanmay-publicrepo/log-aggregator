package dev.tanmay.logagg.connector;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresSinkTask extends SinkTask {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresSinkTask.class);

    private PostgresSinkConfig config;
    private final List<SinkRecord> buffer = new ArrayList<>();
    private long received;
        @Override
    public String version() {
        return PostgresSinkConnector.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        config = new PostgresSinkConfig(props);
        LOG.info("PostgresSinkTask started: table={} batchSize={}", config.tableName(), config.batchSize());
    }
        @Override
    public void put(Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        SinkRecord first = records.iterator().next();
        buffer.addAll(records);
        received += records.size();
        LOG.info("put: received {} records (total {}), first {}-{}@{}", records.size(), received,
                first.topic(), first.kafkaPartition(), first.kafkaOffset());
    }
        @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> currentOffsets) {
        LOG.info("flush: {} buffered records across {} partitions", buffer.size(), currentOffsets.size());
        buffer.clear();
    }

    @Override
    public void stop() {
        LOG.info("PostgresSinkTask stopped after {} records", received);
    }

    long received() {
        return received;
    }

    int buffered() {
        return buffer.size();
    }
}