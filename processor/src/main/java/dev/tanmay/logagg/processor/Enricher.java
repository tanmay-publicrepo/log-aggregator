package dev.tanmay.logagg.processor;

import dev.tanmay.logagg.model.ParsedLogEvent;

public class Enricher {

    private final String region;
    private final String env;

    public Enricher(String region, String env) {
        this.region = region;
        this.env = env;
    }

    public ParsedLogEvent enrich(ParsedLogEvent event) {
        return ParsedLogEvent.newBuilder(event)
                .setRegion(region)
                .setEnv(env)
                .build();
    }
}