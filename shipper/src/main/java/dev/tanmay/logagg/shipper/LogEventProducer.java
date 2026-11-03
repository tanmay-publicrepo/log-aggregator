/** About this class - Produces LogEvent messages to a Kafka topic. It uses a KafkaProducer to send messages and handles serialization using Avro. The producer is configured with properties such as bootstrap servers, schema registry URL, and other Kafka producer settings. It provides methods to send LogEvent messages and track the number of sent messages. The producer can be closed gracefully, ensuring that all pending messages are flushed before closing. */

package dev.tanmay.logagg.shipper;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import dev.tanmay.logagg.model.LogEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

public class LogEventProducer implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LogEventProducer.class);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(10);
    private static final int DELIVERY_TIMEOUT_MS = 120_000;

    private final Producer<String, LogEvent> producer;
    private final String topic;
    private final AtomicLong sent = new AtomicLong();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    public LogEventProducer(ShipperConfig config) {
        this(new KafkaProducer<>(producerProperties(config)), config.topic());
    }

    LogEventProducer(Producer<String, LogEvent> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    static Properties producerProperties(ShipperConfig config) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "shipper");
        props.put(ProducerConfig.LINGER_MS_CONFIG, config.lingerMs());
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, config.batchSizeBytes());
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, config.compressionType());
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, DELIVERY_TIMEOUT_MS);
        return props;
    }

    public void send(LogEvent event) {
        String key = event.getService().toString();
        ProducerRecord<String, LogEvent> record = new ProducerRecord<>(topic, key, event);

        producer.send(record, (metadata, exception) -> {
            if (exception != null) {
                log.error("Failed to send event for service={}", key, exception);
            } else {
                sent.incrementAndGet();
                log.debug("Sent to {}-{} at offset {}",
                        metadata.topic(), metadata.partition(), metadata.offset());
            }
        });
    }

    public long sent() {
        return sent.get();
    }

    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        log.info("Flushing and closing producer ({} records acknowledged)", sent.get());
        producer.flush();
        producer.close(CLOSE_TIMEOUT);
    }
}