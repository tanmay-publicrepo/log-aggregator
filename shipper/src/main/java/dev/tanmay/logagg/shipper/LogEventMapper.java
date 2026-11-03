/**About this class - Maps raw log lines to LogEvent objects. It can parse JSON-formatted log lines and extract fields such as level, message, and trace_id. If the log line is not in JSON format, it treats the entire line as the message and assigns an UNKNOWN level. The class also includes a factory method to create an instance with the local hostname. */


package dev.tanmay.logagg.shipper;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.tanmay.logagg.model.LogEvent;

public class LogEventMapper {

    private static final Logger log = LoggerFactory.getLogger(LogEventMapper.class);
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String UNKNOWN_LEVEL = "UNKNOWN";

    private final String host;

    public LogEventMapper(String host) {
        this.host = host;
    }

    public static LogEventMapper forLocalHost() {
        try {
            return new LogEventMapper(InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            log.warn("Could not resolve local hostname, using 'unknown'", e);
            return new LogEventMapper("unknown");
        }
    }

    public LogEvent toEvent(String rawLine, String service) {
        LogEvent.Builder builder = LogEvent.newBuilder()    
                .setService(service)
                .setHost(host)
                .setRaw(rawLine)
                .setTimestamp(Instant.now());

        String trimmed = rawLine.trim();
        JsonNode json = trimmed.startsWith("{") ? parseJson(trimmed) : null;

        if (json == null) {
            return builder
                    .setLevel(UNKNOWN_LEVEL)
                    .setMessage(trimmed)
                    .build();
        }

        return builder
                .setLevel(json.path("level").asText(UNKNOWN_LEVEL))
                .setMessage(json.path("message").asText(trimmed))
                .setTraceId(json.path("trace_id").asText(null))
                .build();
    }

    private static JsonNode parseJson(String text) {
        try {
            return MAPPER.readTree(text);
        } catch (JsonProcessingException e) {
            return null;
        }
    }
}