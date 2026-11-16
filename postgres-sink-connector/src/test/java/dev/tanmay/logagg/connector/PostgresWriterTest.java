package dev.tanmay.logagg.connector;

import static org.assertj.core.api.Assertions.assertThat;

import java.sql.BatchUpdateException;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Test;

class PostgresWriterTest {

    @Test
    void connectionAndResourceErrorsAreRetriable() {
        assertThat(PostgresWriter.isRetriable(new SQLException("down", "08006"))).isTrue();
        assertThat(PostgresWriter.isRetriable(new SQLException("admin shutdown", "57P01"))).isTrue();
        assertThat(PostgresWriter.isRetriable(new SQLException("deadlock", "40P01"))).isTrue();
        assertThat(PostgresWriter.isRetriable(new SQLTransientConnectionException("pool timeout"))).isTrue();
    }

    @Test
    void dataAndSchemaErrorsAreFatal() {
        assertThat(PostgresWriter.isRetriable(new SQLException("no table", "42P01"))).isFalse();
        assertThat(PostgresWriter.isRetriable(new SQLException("not null", "23502"))).isFalse();
        assertThat(PostgresWriter.isRetriable(new SQLException("no state"))).isFalse();
    }

    @Test
    void chainedCauseIsInspected() {
        BatchUpdateException batch = new BatchUpdateException("batch failed", null, new int[0]);
        batch.setNextException(new SQLException("connection reset", "08006"));
        assertThat(PostgresWriter.isRetriable(batch)).isTrue();
    }
}