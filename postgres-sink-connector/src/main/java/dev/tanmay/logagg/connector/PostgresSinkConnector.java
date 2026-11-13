package dev.tanmay.logagg.connector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresSinkConnector extends SinkConnector {

    static final String VERSION = "0.1.0";
    private static final Logger LOG = LoggerFactory.getLogger(PostgresSinkConnector.class);

    private Map<String, String> props;
        @Override
    public String version() {
        return VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        PostgresSinkConfig config = new PostgresSinkConfig(props);   // throws ConfigException if invalid
        this.props = new HashMap<>(props);
        LOG.info("PostgresSinkConnector {} started, target table {}", VERSION, config.tableName());
    }

    @Override
    public Class<? extends Task> taskClass() {
        return PostgresSinkTask.class;
    }
        @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<>(maxTasks);
        for (int i = 0; i < maxTasks; i++) {
            configs.add(new HashMap<>(props));
        }
        return configs;
    }

    @Override
    public void stop() {
        LOG.info("PostgresSinkConnector stopped");
    }

    @Override
    public ConfigDef config() {
        return PostgresSinkConfig.CONFIG_DEF;
    }
}