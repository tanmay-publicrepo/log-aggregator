package dev.tanmay.logagg.connector;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class PostgresSinkConnectorTest {

    private static final Map<String, String> PROPS = Map.of(
            PostgresSinkConfig.CONNECTION_URL, "jdbc:postgresql://localhost:5432/logs",
            PostgresSinkConfig.CONNECTION_USER, "logs",
            PostgresSinkConfig.CONNECTION_PASSWORD, "logs");

                @Test
    void taskConfigsCopiesPropsForEachTask() {
        PostgresSinkConnector connector = new PostgresSinkConnector();
        connector.start(PROPS);
        List<Map<String, String>> configs = connector.taskConfigs(3);
        assertThat(configs).hasSize(3);
        assertThat(configs).allSatisfy(c -> assertThat(c).containsAllEntriesOf(PROPS));
        assertThat(connector.taskClass()).isEqualTo(PostgresSinkTask.class);
    }

    @Test
    void configExposesAllKeys() {
        assertThat(new PostgresSinkConnector().config().names()).contains(
                PostgresSinkConfig.CONNECTION_URL, PostgresSinkConfig.CONNECTION_PASSWORD,
                PostgresSinkConfig.TABLE_NAME, PostgresSinkConfig.BATCH_SIZE);
        assertThat(new PostgresSinkConnector().version()).isEqualTo("0.1.0");
    }

    @Test
    void startRejectsBadConfig() {
        assertThatThrownBy(() -> new PostgresSinkConnector().start(Map.of()))
                .isInstanceOf(ConfigException.class);
    }
}