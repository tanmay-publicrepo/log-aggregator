package dev.tanmay.logagg.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class PostgresSinkConfigTest {

    private static Map<String, String> minimal() {
        Map<String, String> props = new HashMap<>();
        props.put(PostgresSinkConfig.CONNECTION_URL, "jdbc:postgresql://localhost:5432/logs");
        props.put(PostgresSinkConfig.CONNECTION_USER, "logs");
        props.put(PostgresSinkConfig.CONNECTION_PASSWORD, "secret");
        return props;
    }
        @Test
    void defaultsApply() {
        PostgresSinkConfig config = new PostgresSinkConfig(minimal());
        assertThat(config.tableName()).isEqualTo("parsed_logs");
        assertThat(config.batchSize()).isEqualTo(500);
        assertThat(config.maxRetries()).isEqualTo(10);
        assertThat(config.retryBackoffMs()).isEqualTo(500L);
    }

    @Test
    void passwordIsMasked() {
        PostgresSinkConfig config = new PostgresSinkConfig(minimal());
        assertThat(config.connectionPassword().value()).isEqualTo("secret");
        assertThat(config.connectionPassword().toString()).isEqualTo("[hidden]");
    }

    @Test
    void missingUrlIsRejected() {
        Map<String, String> props = minimal();
        props.remove(PostgresSinkConfig.CONNECTION_URL);
        assertThatThrownBy(() -> new PostgresSinkConfig(props))
                .isInstanceOf(ConfigException.class)
                .hasMessageContaining("connection.url");
    }

    @Test
    void batchSizeMustBePositive() {
        Map<String, String> props = minimal();
        props.put(PostgresSinkConfig.BATCH_SIZE, "0");
        assertThatThrownBy(() -> new PostgresSinkConfig(props)).isInstanceOf(ConfigException.class);
    }
}