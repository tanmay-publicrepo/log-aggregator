package dev.tanmay.logagg.connector;

import static dev.tanmay.logagg.connector.ParsedLogsTableTest.record;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.ArgumentMatchers.anyLong;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTaskContext;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PostgresSinkTaskTest {

    private static final Map<String, String> PROPS = Map.of(
            PostgresSinkConfig.CONNECTION_URL, "jdbc:postgresql://localhost:5432/logs",
            PostgresSinkConfig.CONNECTION_USER, "logs",
            PostgresSinkConfig.CONNECTION_PASSWORD, "logs",
            PostgresSinkConfig.MAX_RETRIES, "2",
            PostgresSinkConfig.RETRY_BACKOFF_MS, "500",
            PostgresSinkConfig.METRICS_ENABLED, "false");   

    private PostgresWriter writer;
    private SinkTaskContext context;
    private PostgresSinkTask task;
    private final List<SinkRecord> batch = List.of(record(0, 10, "service-1", 200, "t-1"), record(1, 3, "service-2", null, null));

    @BeforeEach
    void startTask() {
        writer = mock(PostgresWriter.class);
        context = mock(SinkTaskContext.class);
        task = new PostgresSinkTask(config -> writer);
        task.initialize(context);
        task.start(PROPS);
    }

    @Test
    void putWritesTheBatchOnceAndCountsRows() throws SQLException {
        when(writer.write(anyCollection())).thenReturn(2);
        task.put(batch);
        verify(writer).write(batch);
        assertThat(task.received()).isEqualTo(2);
        assertThat(task.written()).isEqualTo(2);
        assertThat(task.attempt()).isZero();
    }

    @Test
    void tombstonesAreSkippedAndEmptyBatchesNeverReachTheWriter() throws SQLException {
        SinkRecord tombstone = new SinkRecord("logs.parsed", 0, null, "service-1", null, null, 99);
        task.put(List.of(tombstone));
        task.put(List.of());
        verify(writer, never()).write(anyCollection());
        assertThat(task.received()).isEqualTo(1);
    }
        @Test
    void retriableErrorSetsTimeoutAndAsksForTheSameBatch() throws SQLException {
        when(writer.write(anyCollection())).thenThrow(new SQLException("down", "08006"));
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(RetriableException.class);
        verify(context).timeout(500L);
        assertThat(task.attempt()).isEqualTo(1);
        assertThat(task.written()).isZero();
    }

    @Test
    void backoffDoublesPerConsecutiveFailure() throws SQLException {
        when(writer.write(anyCollection())).thenThrow(new SQLException("down", "08006"));
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(RetriableException.class);
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(RetriableException.class);
        verify(context).timeout(500L);
        verify(context).timeout(1000L);
    }

    @Test
    void exhaustedRetriesBecomeConnectException() throws SQLException {
        when(writer.write(anyCollection())).thenThrow(new SQLException("down", "08006"));
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(RetriableException.class);
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(RetriableException.class);
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class);
    }

    @Test
    void fatalErrorFailsImmediately() throws SQLException {
        when(writer.write(anyCollection())).thenThrow(new SQLException("no table", "42P01"));
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class);
        verify(context, never()).timeout(anyLong());
    }

    @Test
    void successResetsTheRetryBudget() throws SQLException {
        when(writer.write(anyCollection())).thenThrow(new SQLException("down", "08006")).thenReturn(2)
                .thenThrow(new SQLException("down", "08006"));
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(RetriableException.class);
        task.put(batch);
        assertThatThrownBy(() -> task.put(batch)).isInstanceOf(RetriableException.class);
        verify(context, org.mockito.Mockito.times(2)).timeout(500L);
    }

    @Test
    void stopClosesTheWriter() {
        task.stop();
        verify(writer).close();
    }
}