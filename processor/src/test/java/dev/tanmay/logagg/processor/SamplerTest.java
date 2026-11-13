package dev.tanmay.logagg.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import dev.tanmay.logagg.model.ParsedLogEvent;

class SamplerTest {

    @Test
    void otherServicesAlwaysPass() {
        Sampler sampler = new Sampler("service-4", 0.1, () -> 0.99);
        assertThat(sampler.keep(event("service-1"))).isTrue();
        assertThat(sampler.mark(event("service-1")).getSampled()).isFalse();
    }

    @Test
    void sampledServiceKeptWhenRandomBelowRate() {
        Sampler sampler = new Sampler("service-4", 0.1, () -> 0.05);
        assertThat(sampler.keep(event("service-4"))).isTrue();
        assertThat(sampler.mark(event("service-4")).getSampled()).isTrue();
        assertThat(sampler.dropped()).isZero();
    }

    @Test
    void sampledServiceDroppedWhenRandomAboveRate() {
        Sampler sampler = new Sampler("service-4", 0.1, () -> 0.95);
        assertThat(sampler.keep(event("service-4"))).isFalse();
        assertThat(sampler.dropped()).isEqualTo(1);
    }

    static ParsedLogEvent event(String service) {
        return ParsedLogEvent.newBuilder()
                .setService(service).setHost("h").setEventTime(Instant.now()).setIngestTime(Instant.now())
                .setLevel("INFO").setMessage("m").setFormat("JSON").setRaw("m")
                .build();
    }
}