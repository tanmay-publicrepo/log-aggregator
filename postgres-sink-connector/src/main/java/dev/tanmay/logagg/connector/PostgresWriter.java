package dev.tanmay.logagg.connector;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import java.util.Collection;
import java.util.List;
import javax.sql.DataSource;
import org.apache.kafka.connect.sink.SinkRecord;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PostgresWriter implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PostgresWriter.class);
    static final List<String> RETRIABLE_SQLSTATE_PREFIXES = List.of("08", "40", "53", "57");
        private final DataSource dataSource;
    private final String upsertSql;
    private final int batchSize;

    PostgresWriter(PostgresSinkConfig config) {
        this(pool(config), config.tableName(), config.batchSize());
    }

    PostgresWriter(DataSource dataSource, String table, int batchSize) {
        this.dataSource = dataSource;
        this.upsertSql = ParsedLogsTable.upsertSql(table);
        this.batchSize = batchSize;
    }

    private static HikariDataSource pool(PostgresSinkConfig config) {
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(config.connectionUrl());
        hikari.setUsername(config.connectionUser());
        hikari.setPassword(config.connectionPassword().value());
        hikari.setDriverClassName("org.postgresql.Driver");
        hikari.setMaximumPoolSize(1);
        hikari.setAutoCommit(false);
        hikari.setConnectionTimeout(5_000);
        hikari.setInitializationFailTimeout(-1);
        hikari.setPoolName("postgres-sink");
        hikari.addDataSourceProperty("socketTimeout", "30");
        return new HikariDataSource(hikari);
    }
        int write(Collection<SinkRecord> records) throws SQLException {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(upsertSql)) {
            try {
                int inserted = 0;
                int pending = 0;
                for (SinkRecord record : records) {
                    ParsedLogsTable.bind(statement, record);
                    statement.addBatch();
                    if (++pending == batchSize) {
                        inserted += countInserted(statement.executeBatch());
                        pending = 0;
                    }
                }
                if (pending > 0) {
                    inserted += countInserted(statement.executeBatch());
                }
                connection.commit();
                return inserted;
            } catch (SQLException e) {
                rollbackQuietly(connection);
                throw e;
            }
        }
    }
        private static int countInserted(int[] counts) {
        int inserted = 0;
        for (int count : counts) {
            if (count > 0) {
                inserted++;
            }
        }
        return inserted;
    }

    private static void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException e) {
            LOG.warn("rollback failed: {}", e.getMessage());
        }
    }

    static boolean isRetriable(SQLException e) {
        for (SQLException current = e; current != null; current = current.getNextException()) {
            if (current instanceof SQLTransientException || current instanceof SQLRecoverableException) {
                return true;
            }
            String state = current.getSQLState();
            if (state != null && state.length() >= 2) {
                return RETRIABLE_SQLSTATE_PREFIXES.contains(state.substring(0, 2));
            }
        }
        return false;
    }

    static Connection openConnection(PostgresSinkConfig config) throws SQLException {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(config.connectionUrl());
        dataSource.setUser(config.connectionUser());
        dataSource.setPassword(config.connectionPassword().value());
        return dataSource.getConnection();
    }

    @Override
    public void close() {
        if (dataSource instanceof HikariDataSource hikari) {
            hikari.close();
        }
    }
}