package dev.tanmay.logagg.processor;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.tanmay.logagg.model.DeadLetterEvent;
import dev.tanmay.logagg.model.LogEvent;
import dev.tanmay.logagg.model.ParsedLogEvent;

public class LineParser {

    static final String FORMAT_JSON = "JSON";
    static final String FORMAT_TEXT = "TEXT";
    static final String STAGE_PARSE = "parse";

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Pattern TEXT_LINE = Pattern.compile(
            "^(?<ts>\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) "
            + "(?<level>[A-Z]+) "
            + "\\[(?<service>[^\\]]+)\\] "
            + "(?<host>\\S+) "
            + "(?<rest>.*)$");

    private static final Pattern KEY_VALUE = Pattern.compile("\\b(?<key>[a-z_]+)=(?<value>\\S+)");

    private static final DateTimeFormatter TEXT_TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS");

    public ParseOutcome parse(LogEvent in) {
        String trimmed = in.getRaw().trim();
        if (trimmed.isEmpty()) {
            return failure(in, "empty line");
        }
        try {
            if (trimmed.startsWith("{")) {
                return parseJson(in, trimmed);
            }
            return parseText(in, trimmed);
        } catch (RuntimeException e) {
            return failure(in, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    private ParseOutcome parseJson(LogEvent in, String text) {
        JsonNode node;
        try {
            node = MAPPER.readTree(text);
        } catch (JsonProcessingException e) {
            return failure(in, "invalid json: " + e.getOriginalMessage());
        }
        if (!node.isObject()) {
            return failure(in, "json is not an object");
        }
        String timestampText = node.path("timestamp").asText(null);
        String level = node.path("level").asText(null);
        String message = node.path("message").asText(null);
        if (timestampText == null || level == null || message == null) {
            return failure(in, "json is missing one of timestamp, level, message");
        }
        Instant eventTime;
        try {
            eventTime = OffsetDateTime.parse(timestampText).toInstant();
        } catch (DateTimeParseException e) {
            return failure(in, "unparseable json timestamp: " + timestampText);
        }

        ParsedLogEvent.Builder b = baseBuilder(in, FORMAT_JSON)
                .setEventTime(eventTime)
                .setLevel(level)
                .setMessage(message)
                .setTraceId(node.path("trace_id").asText(null));
        if (node.hasNonNull("service")) {
            b.setService(node.get("service").asText());
        }
        if (node.hasNonNull("host")) {
            b.setHost(node.get("host").asText());
        }
        if (node.path("status_code").isIntegralNumber()) {
            b.setStatusCode(node.get("status_code").asInt());
        }
        if (node.path("latency").isIntegralNumber()) {
            b.setLatencyMs(node.get("latency").asLong());
        }
        return ParseOutcome.ok(b.build());
    }

    private ParseOutcome parseText(LogEvent in, String text) {
        Matcher m = TEXT_LINE.matcher(text);
        if (!m.matches()) {
            return failure(in, "text line does not match the expected format");
        }
        Instant eventTime;
        try {
            eventTime = LocalDateTime.parse(m.group("ts"), TEXT_TIMESTAMP).toInstant(ZoneOffset.UTC);
        } catch (DateTimeParseException e) {
            return failure(in, "unparseable text timestamp: " + m.group("ts"));
        }
        String rest = m.group("rest");

        ParsedLogEvent.Builder b = baseBuilder(in, FORMAT_TEXT)
                .setEventTime(eventTime)
                .setLevel(m.group("level"))
                .setService(m.group("service"))
                .setHost(m.group("host"))
                .setMessage(rest);

        Matcher kv = KEY_VALUE.matcher(rest);
        while (kv.find()) {
            String key = kv.group("key");
            String value = kv.group("value");
            switch (key) {
                case "status" -> b.setStatusCode(parseIntOrNull(value));
                case "latency" -> b.setLatencyMs(parseLatencyMs(value));
                case "trace" -> b.setTraceId(value);
                default -> { }
            }
        }
        return ParseOutcome.ok(b.build());
    }

    private ParsedLogEvent.Builder baseBuilder(LogEvent in, String format) {
        return ParsedLogEvent.newBuilder()
                .setService(in.getService())
                .setHost(in.getHost())
                .setIngestTime(in.getTimestamp())
                .setFormat(format)
                .setRaw(in.getRaw());
    }

    private ParseOutcome failure(LogEvent in, String reason) {
        DeadLetterEvent dead = DeadLetterEvent.newBuilder()
                .setService(in.getService())
                .setHost(in.getHost())
                .setIngestTime(in.getTimestamp())
                .setFailedAt(Instant.now())
                .setStage(STAGE_PARSE)
                .setError(reason)
                .setRaw(in.getRaw())
                .build();
        return ParseOutcome.failed(dead);
    }

    private static Integer parseIntOrNull(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    private static Long parseLatencyMs(String value) {
        String digits = value.endsWith("ms") ? value.substring(0, value.length() - 2) : value;
        try {
            return Long.parseLong(digits);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}