package dev.tanmay.logagg.processor;

import dev.tanmay.logagg.model.DeadLetterEvent;
import dev.tanmay.logagg.model.ParsedLogEvent;

public record ParseOutcome(ParsedLogEvent event, DeadLetterEvent deadLetter) {

    public static ParseOutcome ok(ParsedLogEvent event) {
        return new ParseOutcome(event, null);
    }

    public static ParseOutcome failed(DeadLetterEvent deadLetter) {
        return new ParseOutcome(null, deadLetter);
    }

    public boolean isSuccess() {
        return event != null;
    }
}