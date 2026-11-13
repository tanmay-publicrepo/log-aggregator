package dev.tanmay.logagg.processor;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;

import dev.tanmay.logagg.model.ParsedLogEvent;

public class Sampler {

    private final String sampleService;
    private final double sampleRate;
    private final DoubleSupplier random;
    private final AtomicLong dropped = new AtomicLong();

    public Sampler(String sampleService, double sampleRate, DoubleSupplier random) {
        if (sampleRate < 0.0 || sampleRate > 1.0) {
            throw new IllegalArgumentException("sampleRate must be between 0 and 1, got " + sampleRate);
        }
        this.sampleService = sampleService;
        this.sampleRate = sampleRate;
        this.random = random;
    }

        public boolean keep(ParsedLogEvent event) {
        if (!sampleService.equals(event.getService())) {
            return true;
        }
        boolean keep = random.getAsDouble() < sampleRate;
        if (!keep) {
            dropped.incrementAndGet();
        }
        return keep;
    }

    public ParsedLogEvent mark(ParsedLogEvent event) {
        if (!sampleService.equals(event.getService())) {
            return event;
        }
        return ParsedLogEvent.newBuilder(event).setSampled(true).build();
    }

    public long dropped() {
        return dropped.get();
    }
}