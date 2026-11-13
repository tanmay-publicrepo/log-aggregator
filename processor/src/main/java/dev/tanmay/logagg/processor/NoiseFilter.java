package dev.tanmay.logagg.processor;

import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import dev.tanmay.logagg.model.ParsedLogEvent;

public class NoiseFilter {

    static final String DEBUG = "DEBUG";

    private final Set<String> noisyServices;
    private final AtomicLong dropped = new AtomicLong();

    public NoiseFilter(Set<String> noisyServices) {
        this.noisyServices = Set.copyOf(noisyServices);
    }

    public boolean keep(ParsedLogEvent event) {
        boolean noisyDebug = DEBUG.equals(event.getLevel()) && noisyServices.contains(event.getService());
        if (noisyDebug) {
            dropped.incrementAndGet();
        }
        return !noisyDebug;
    }

    public long dropped() {
        return dropped.get();
    }
}