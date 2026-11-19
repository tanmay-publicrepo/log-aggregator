package dev.tanmay.logagg.connector;

import static dev.tanmay.logagg.connector.ParsedLogsTableTest.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
@Testcontainers(disabledWithoutDocker = true)
class PostgresSinkTaskIT {

    @Container
    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16")
            .withDatabaseName("logs").withUsername("logs").withPassword("logs");

    private Map<String, String> props;
    private PostgresSinkConfig config;
    private SinkTaskContext context;
    private final List<SinkRecord> batch = List.of(
            record(0, 10, "service-1", 200, "t-1"), record(0, 11, "service-1", 503, "t-2"), record(3, 7, "service-2", null, null));

    @BeforeEach
    void freshTable() throws SQLException {
        props = Map.of(PostgresSinkConfig.CONNECTION_URL, POSTGRES.getJdbcUrl(),
                PostgresSinkConfig.CONNECTION_USER, POSTGRES.getUsername(),
                PostgresSinkConfig.CONNECTION_PASSWORD, POSTGRES.getPassword(),
                PostgresSinkConfig.MAX_RETRIES, "3",
            PostgresSinkConfig.METRICS_ENABLED, "false");
        config = new PostgresSinkConfig(props);
        context = mock(SinkTaskContext.class);
        execute("DROP TABLE IF EXISTS parsed_logs");
        try (Connection connection = PostgresWriter.openConnection(config)) {
            ParsedLogsTable.ensure(connection, config.tableName());
        }
    }

    static final class FlakyWriter extends PostgresWriter {
        private boolean failedOnce;
        FlakyWriter(PostgresSinkConfig config) { super(config); }
        @Override int write(Collection<SinkRecord> records) throws SQLException {
            if (!failedOnce) { failedOnce = true; throw new SQLException("simulated network drop", "08006"); }
            return super.write(records);
        }
    }
        @Test
    void rowsLandExactlyOnceAfterAForcedRetryAndARedelivery() throws SQLException {
        PostgresSinkTask task = new PostgresSinkTask(c -> new FlakyWriter(c));
        task.initialize(context);
        task.start(props);
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(RetriableException.class);
        verify(context).timeout(500L);
        assertThat(count()).isZero();
        task.put(batch);                       // what the worker does after the timeout
        assertThat(count()).isEqualTo(3);
        task.put(batch);                       // redelivery after a crash before the offset commit
        assertThat(count()).isEqualTo(3);
        assertThat(task.written()).isEqualTo(3);
        assertThat(task.received()).isEqualTo(9);
        task.stop();
    }

    @Test
    void fatalErrorRollsBackTheWholeBatch() throws SQLException {
        execute("ALTER TABLE parsed_logs ADD CONSTRAINT no_server_errors CHECK (status_code < 500)");
        PostgresSinkTask task = new PostgresSinkTask(PostgresWriter::new);
        task.initialize(context);
        task.start(props);
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(ConnectException.class).isNotInstanceOf(RetriableException.class);
        assertThat(count()).isZero();          // the two good rows were rolled back with the bad one
        execute("ALTER TABLE parsed_logs DROP CONSTRAINT no_server_errors");
        task.put(batch);
        assertThat(count()).isEqualTo(3);
        task.stop();
    }

    private long count() throws SQLException {
        try (Connection connection = PostgresWriter.openConnection(config);
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery("SELECT count(*) FROM parsed_logs")) {
            rows.next();
            return rows.getLong(1);
        }
    }

    private void execute(String sql) throws SQLException {
        try (Connection connection = PostgresWriter.openConnection(config); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }
}