package dev.tanmay.logagg.connector;

import java.util.Map;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Range;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.types.Password;

public class PostgresSinkConfig extends AbstractConfig {

    public static final String CONNECTION_URL = "connection.url";
    public static final String CONNECTION_USER = "connection.user";
    public static final String CONNECTION_PASSWORD = "connection.password";
    public static final String TABLE_NAME = "table.name";
    public static final String BATCH_SIZE = "batch.size";
    public static final String MAX_RETRIES = "max.retries";
    public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";


    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define(CONNECTION_URL, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                    "JDBC URL, e.g. jdbc:postgresql://postgres:5432/logs")
            .define(CONNECTION_USER, Type.STRING, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                    "Database user")
            .define(CONNECTION_PASSWORD, Type.PASSWORD, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                    "Database password; masked in logs")
            .define(TABLE_NAME, Type.STRING, "parsed_logs", Importance.MEDIUM,
                    "Target table, one row per parsed log event")
            .define(BATCH_SIZE, Type.INT, 500, Range.atLeast(1), Importance.MEDIUM,
                    "Maximum rows per INSERT batch")
            .define(MAX_RETRIES, Type.INT, 10, Range.atLeast(0), Importance.LOW,
                    "Retries of a failed batch before the task fails")
            .define(RETRY_BACKOFF_MS, Type.LONG, 500L, Range.atLeast(0), Importance.LOW,
                    "Pause between retries in milliseconds");

        public PostgresSinkConfig(Map<String, String> props) {
        super(CONFIG_DEF, props);
    }

    public String connectionUrl()        { return getString(CONNECTION_URL); }
    public String connectionUser()       { return getString(CONNECTION_USER); }
    public Password connectionPassword() { return getPassword(CONNECTION_PASSWORD); }
    public String tableName()            { return getString(TABLE_NAME); }
    public int batchSize()               { return getInt(BATCH_SIZE); }
    public int maxRetries()              { return getInt(MAX_RETRIES); }
    public long retryBackoffMs()         { return getLong(RETRY_BACKOFF_MS); }
}                