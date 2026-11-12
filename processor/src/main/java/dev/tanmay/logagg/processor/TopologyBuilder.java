package dev.tanmay.logagg.processor;

import java.util.Map;

import org.apache.avro.specific.SpecificRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Branched;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;

import dev.tanmay.logagg.model.DeadLetterEvent;
import dev.tanmay.logagg.model.LogEvent;
import dev.tanmay.logagg.model.ParsedLogEvent;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

public class TopologyBuilder {

    static final String BRANCH_PREFIX = "parse-";
    static final String BRANCH_OK = BRANCH_PREFIX + "ok";
    static final String BRANCH_FAILED = BRANCH_PREFIX + "failed";

    private final ProcessorConfig config;
    private final LineParser parser;
    private final Enricher enricher;
    private final NoiseFilter noiseFilter;
    private final Sampler sampler;

    public TopologyBuilder(ProcessorConfig config, LineParser parser, Enricher enricher,
                           NoiseFilter noiseFilter, Sampler sampler) {
        this.config = config;
        this.parser = parser;
        this.enricher = enricher;
        this.noiseFilter = noiseFilter;
        this.sampler = sampler;
    }

    public Topology build() {
        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, LogEvent> raw = builder.stream(
                config.inputTopic(),
                Consumed.with(Serdes.String(), avroSerde(LogEvent.class)));

        Map<String, KStream<String, ParseOutcome>> branches = raw
                .mapValues(parser::parse, Named.as("parse-line"))
                .split(Named.as(BRANCH_PREFIX))
                .branch((key, outcome) -> outcome.isSuccess(), Branched.as("ok"))
                .defaultBranch(Branched.as("failed"));

        branches.get(BRANCH_OK)
                .mapValues(ParseOutcome::event, Named.as("unwrap-parsed"))
                .mapValues(enricher::enrich, Named.as("enrich"))
                .filter((key, event) -> noiseFilter.keep(event), Named.as("drop-noisy-debug"))
                .filter((key, event) -> sampler.keep(event), Named.as("sample"))
                .mapValues(sampler::mark, Named.as("mark-sampled"))
                .to(config.parsedTopic(), Produced.with(Serdes.String(), avroSerde(ParsedLogEvent.class)));

        branches.get(BRANCH_FAILED)
                .mapValues(ParseOutcome::deadLetter, Named.as("unwrap-dead-letter"))
                .to(config.dlqTopic(), Produced.with(Serdes.String(), avroSerde(DeadLetterEvent.class)));

        return builder.build();
    }

    <T extends SpecificRecord> SpecificAvroSerde<T> avroSerde(Class<T> type) {
        SpecificAvroSerde<T> serde = new SpecificAvroSerde<>();
        serde.configure(
                Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, config.schemaRegistryUrl()),
                false);
        return serde;
    }
}