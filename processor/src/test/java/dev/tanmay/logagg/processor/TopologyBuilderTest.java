package dev.tanmay.logagg.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import dev.tanmay.logagg.model.DeadLetterEvent;
import dev.tanmay.logagg.model.LogEvent;
import dev.tanmay.logagg.model.ParsedLogEvent;
import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;

class TopologyBuilderTest {

    private static final String REGISTRY_SCOPE = "topology-test";

    private TopologyTestDriver driver;
    private TestInputTopic<String, LogEvent> input;
    private TestOutputTopic<String, ParsedLogEvent> parsed;
    private TestOutputTopic<String, DeadLetterEvent> dlq;
    private double nextRandom = 0.5;

    @BeforeEach
    void setUp() {
        ProcessorConfig config = ProcessorConfig.from(Map.of(
                "SCHEMA_REGISTRY_URL", "mock://" + REGISTRY_SCOPE,
                "NOISY_SERVICES", "service-3",
                "SAMPLE_SERVICE", "service-4",
                "SAMPLE_RATE", "0.1",
                "ENRICH_REGION", "test-region",
                "ENRICH_ENV", "test"));
        Sampler sampler = new Sampler(config.sampleService(), config.sampleRate(), () -> nextRandom);
        TopologyBuilder builder = new TopologyBuilder(config, new LineParser(),
                new Enricher(config.region(), config.env()), new NoiseFilter(config.noisyServices()), sampler);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "topology-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        driver = new TopologyTestDriver(builder.build(), props);

        input = driver.createInputTopic(config.inputTopic(), new StringSerializer(),
                builder.avroSerde(LogEvent.class).serializer());
        parsed = driver.createOutputTopic(config.parsedTopic(), new StringDeserializer(),
                builder.avroSerde(ParsedLogEvent.class).deserializer());
        dlq = driver.createOutputTopic(config.dlqTopic(), new StringDeserializer(),
                builder.avroSerde(DeadLetterEvent.class).deserializer());
    }

    @AfterEach
    void tearDown() {
        driver.close();
        MockSchemaRegistry.dropScope(REGISTRY_SCOPE);
    }

    @Test
    void jsonLineLandsInParsedWithEnrichment() {
        input.pipeInput("service-1", logEvent("service-1", LineParserTest.JSON_LINE));

        KeyValue<String, ParsedLogEvent> out = parsed.readKeyValue();
        assertThat(out.key).isEqualTo("service-1");
        assertThat(out.value.getLevel()).isEqualTo("INFO");
        assertThat(out.value.getRegion()).isEqualTo("test-region");
        assertThat(out.value.getEnv()).isEqualTo("test");
        assertThat(out.value.getSampled()).isFalse();
        assertThat(dlq.isEmpty()).isTrue();
    }

    @Test
    void malformedLineLandsInDlq() {
        input.pipeInput("service-1", logEvent("service-1", "{\"timestamp\": \"2026-"));

        assertThat(parsed.isEmpty()).isTrue();
        DeadLetterEvent dead = dlq.readValue();
        assertThat(dead.getError()).startsWith("invalid json");
        assertThat(dead.getRaw()).isEqualTo("{\"timestamp\": \"2026-");
    }

    @Test
    void noisyDebugIsDropped() {
        String debugLine = "{\"timestamp\": \"2026-10-21T05:06:11.482+00:00\", \"level\": \"DEBUG\", "
                + "\"service\": \"service-3\", \"message\": \"cache hit\"}";
        input.pipeInput("service-3", logEvent("service-3", debugLine));

        assertThat(parsed.isEmpty()).isTrue();
        assertThat(dlq.isEmpty()).isTrue();
    }

    @Test
    void sampledServiceIsFlaggedWhenKept() {
        nextRandom = 0.05;
        input.pipeInput("service-4", logEvent("service-4", LineParserTest.TEXT_LINE.replace("service-2", "service-4")));

        assertThat(parsed.readValue().getSampled()).isTrue();
    }

    @Test
    void sampledServiceIsDroppedWhenNotKept() {
        nextRandom = 0.95;
        input.pipeInput("service-4", logEvent("service-4", LineParserTest.TEXT_LINE.replace("service-2", "service-4")));

        assertThat(parsed.isEmpty()).isTrue();
        assertThat(dlq.isEmpty()).isTrue();
    }

    private static LogEvent logEvent(String service, String raw) {
        return LogEvent.newBuilder()
                .setService(service).setHost("shipper-host").setTimestamp(Instant.now())
                .setLevel("UNKNOWN").setMessage(raw).setRaw(raw)
                .build();
    }
}