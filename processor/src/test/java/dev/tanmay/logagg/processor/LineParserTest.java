package dev.tanmay.logagg.processor;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;

import org.junit.jupiter.api.Test;

import dev.tanmay.logagg.model.LogEvent;
import dev.tanmay.logagg.model.ParsedLogEvent;

class LineParserTest {

    public static final String JSON_LINE = "{\"timestamp\": \"2026-10-21T05:06:11.482+00:00\", \"level\": \"INFO\", "
            + "\"service\": \"service-1\", \"host\": \"service-1-host-2\", \"message\": \"order placed order=4411\", "
            + "\"trace_id\": \"7f3a9c1e2b4d6f80\", \"status_code\": 201, \"latency\": 143}";

    public static final String TEXT_LINE = "2026-10-21 05:06:11,490 ERROR [service-2] service-2-host-1 "
            + "payment declined order=8812 reason=card_expired status=502 latency=911ms trace=0c3e9a7b5d1f2e46";

    private final LineParser parser = new LineParser();

    @Test
    void jsonLineParsesAllFields() {
        ParseOutcome outcome = parser.parse(logEvent(JSON_LINE));

        assertThat(outcome.isSuccess()).isTrue();
        ParsedLogEvent e = outcome.event();
        assertThat(e.getFormat()).isEqualTo("JSON");
        assertThat(e.getLevel()).isEqualTo("INFO");
        assertThat(e.getService()).isEqualTo("service-1");
        assertThat(e.getHost()).isEqualTo("service-1-host-2");
        assertThat(e.getMessage()).isEqualTo("order placed order=4411");
        assertThat(e.getTraceId()).isEqualTo("7f3a9c1e2b4d6f80");
        assertThat(e.getStatusCode()).isEqualTo(201);
        assertThat(e.getLatencyMs()).isEqualTo(143L);
        assertThat(e.getEventTime()).isEqualTo(Instant.parse("2026-10-21T05:06:11.482Z"));
        assertThat(e.getRaw()).isEqualTo(JSON_LINE);
    }

    @Test
    void textLineParsesWithRegex() {
        ParseOutcome outcome = parser.parse(logEvent(TEXT_LINE));

        assertThat(outcome.isSuccess()).isTrue();
        ParsedLogEvent e = outcome.event();
        assertThat(e.getFormat()).isEqualTo("TEXT");
        assertThat(e.getLevel()).isEqualTo("ERROR");
        assertThat(e.getService()).isEqualTo("service-2");
        assertThat(e.getHost()).isEqualTo("service-2-host-1");
        assertThat(e.getMessage()).startsWith("payment declined");
        assertThat(e.getEventTime()).isEqualTo(Instant.parse("2026-10-21T05:06:11.490Z"));
    }

    @Test
    void keyValuePairsBecomeTypedFields() {
        ParsedLogEvent e = parser.parse(logEvent(TEXT_LINE)).event();

        assertThat(e.getStatusCode()).isEqualTo(502);
        assertThat(e.getLatencyMs()).isEqualTo(911L);
        assertThat(e.getTraceId()).isEqualTo("0c3e9a7b5d1f2e46");
    }

    @Test
    void malformedJsonGoesToDeadLetter() {
        ParseOutcome outcome = parser.parse(logEvent("{\"timestamp\": \"2026-"));

        assertThat(outcome.isSuccess()).isFalse();
        assertThat(outcome.deadLetter().getError()).startsWith("invalid json");
        assertThat(outcome.deadLetter().getStage()).isEqualTo("parse");
        assertThat(outcome.deadLetter().getRaw()).isEqualTo("{\"timestamp\": \"2026-");
    }

    @Test
    void unrecognizedTextGoesToDeadLetter() {
        ParseOutcome outcome = parser.parse(logEvent("something the regex has never seen"));

        assertThat(outcome.isSuccess()).isFalse();
        assertThat(outcome.deadLetter().getError()).contains("does not match");
    }

    @Test
    void emptyLineGoesToDeadLetter() {
        assertThat(parser.parse(logEvent("   ")).deadLetter().getError()).isEqualTo("empty line");
    }

    private static LogEvent logEvent(String raw) {
        return LogEvent.newBuilder()
                .setService("service-9")
                .setHost("shipper-host")
                .setTimestamp(Instant.now())
                .setLevel("UNKNOWN")
                .setMessage(raw)
                .setRaw(raw)
                .build();
    }
}