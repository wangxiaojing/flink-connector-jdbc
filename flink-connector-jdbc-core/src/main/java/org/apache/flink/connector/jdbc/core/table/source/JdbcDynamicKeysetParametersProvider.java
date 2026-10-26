/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.jdbc.core.table.source;

import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.datasource.connections.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;
import org.apache.flink.connector.jdbc.split.JdbcParameterValuesProvider;

import org.apache.commons.lang3.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientConnectionException;
import java.sql.SQLTransientException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Discovers keyset boundaries from actual rows for sparse MySQL partition keys. */
final class JdbcDynamicKeysetParametersProvider implements JdbcParameterValuesProvider {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG =
            LoggerFactory.getLogger(JdbcDynamicKeysetParametersProvider.class);
    private static final int PROGRESS_LOG_INTERVAL = 1_000;

    private final InternalJdbcConnectionOptions connectionOptions;
    private final String quotedTable;
    private final String quotedPartitionColumn;
    private final int chunkSize;
    private final int maxSplits;
    private final int discoveryMaxRetries;
    private final Duration discoveryRetryBackoff;
    private final Duration discoveryQueryTimeout;
    private final Duration discoveryTotalTimeout;
    private final List<String> predicates;
    private final Serializable[] predicateParameters;

    JdbcDynamicKeysetParametersProvider(
            InternalJdbcConnectionOptions connectionOptions,
            JdbcDialect dialect,
            String partitionColumn,
            int chunkSize,
            int maxSplits,
            int discoveryMaxRetries,
            Duration discoveryRetryBackoff,
            Duration discoveryQueryTimeout,
            Duration discoveryTotalTimeout,
            List<String> predicates,
            Serializable[] predicateParameters) {
        this.connectionOptions = connectionOptions;
        this.quotedTable = dialect.quoteIdentifier(connectionOptions.getTableName());
        this.quotedPartitionColumn = dialect.quoteIdentifier(partitionColumn);
        this.chunkSize = chunkSize;
        this.maxSplits = maxSplits;
        this.discoveryMaxRetries = discoveryMaxRetries;
        this.discoveryRetryBackoff = discoveryRetryBackoff;
        this.discoveryQueryTimeout = discoveryQueryTimeout;
        this.discoveryTotalTimeout = discoveryTotalTimeout;
        this.predicates = new ArrayList<>(predicates);
        this.predicateParameters = predicateParameters.clone();
    }

    @Override
    public Serializable[][] getParameterValues() {
        JdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(connectionOptions);
        try {
            return discoverParameterValues(provider);
        } catch (SQLException | ClassNotFoundException e) {
            throw new IllegalStateException("Failed to discover dynamic JDBC partitions.", e);
        } finally {
            provider.closeConnection();
        }
    }

    Serializable[][] discoverParameterValues(Connection connection) throws SQLException {
        try {
            return discoverParameterValues(new FixedConnectionProvider(connection));
        } catch (ClassNotFoundException impossible) {
            throw new SQLException("Unexpected JDBC driver loading failure.", impossible);
        }
    }

    Serializable[][] discoverParameterValues(JdbcConnectionProvider provider)
            throws SQLException, ClassNotFoundException {
        long startedAtNanos = System.nanoTime();
        String minQuery = buildMinQuery();
        String boundaryQuery = buildNextBoundaryQuery();
        String nextValueQuery = buildNextValueQuery();
        try (DiscoveryQueryExecutor executor =
                new DiscoveryQueryExecutor(
                        provider, minQuery, boundaryQuery, nextValueQuery, startedAtNanos)) {
            Object min = executor.query(QueryKind.MINIMUM, predicateParameters);
            if (min == null) {
                LOG.info(
                        "Dynamic JDBC partition discovery found no matching rows in {}.",
                        quotedTable);
                return new Serializable[0][];
            }

            List<Serializable[]> partitions = new ArrayList<>();
            Object start = min;
            while (true) {
                Object end = queryNextBoundary(executor, start);
                if (end == null) {
                    addPartition(partitions, dataQueryParameters(start, null), startedAtNanos);
                    break;
                }
                if (compare(start, end) >= 0) {
                    end = queryNextValue(executor, start);
                }
                if (end == null) {
                    addPartition(partitions, dataQueryParameters(start, null), startedAtNanos);
                    break;
                }
                addPartition(partitions, dataQueryParameters(start, end), startedAtNanos);
                start = end;
            }
            LOG.info(
                    "Discovered {} dynamic JDBC partitions for {} in {} ms.",
                    partitions.size(),
                    quotedTable,
                    elapsedMillis(startedAtNanos));
            return partitions.toArray(new Serializable[0][]);
        }
    }

    private Object queryNextBoundary(DiscoveryQueryExecutor executor, Object start)
            throws SQLException, ClassNotFoundException {
        Serializable[] parameters = ArrayUtils.add(predicateParameters, asSerializable(start));
        return executor.query(QueryKind.NEXT_BOUNDARY, parameters);
    }

    private Object queryNextValue(DiscoveryQueryExecutor executor, Object start)
            throws SQLException, ClassNotFoundException {
        Serializable[] parameters = ArrayUtils.add(predicateParameters, asSerializable(start));
        return executor.query(QueryKind.NEXT_DISTINCT_VALUE, parameters);
    }

    private Serializable[] dataQueryParameters(Object start, Object end) {
        Serializable serializableStart = asSerializable(start);
        Serializable serializableEnd = end == null ? null : asSerializable(end);
        return ArrayUtils.addAll(
                new Serializable[] {serializableStart, serializableEnd, serializableEnd},
                predicateParameters);
    }

    private void addPartition(
            List<Serializable[]> partitions, Serializable[] partition, long startedAtNanos) {
        if (partitions.size() >= maxSplits) {
            throw new IllegalStateException(
                    "Dynamic JDBC partition discovery exceeded scan.partition.max-splits="
                            + maxSplits
                            + " for "
                            + quotedTable
                            + ". Increase scan.partition.chunk-size or "
                            + "scan.partition.max-splits.");
        }
        partitions.add(partition);
        if (partitions.size() % PROGRESS_LOG_INTERVAL == 0) {
            LOG.info(
                    "Discovered {} dynamic JDBC partitions for {} after {} ms.",
                    partitions.size(),
                    quotedTable,
                    elapsedMillis(startedAtNanos));
        }
    }

    private static long elapsedMillis(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000;
    }

    private enum QueryKind {
        MINIMUM("minimum"),
        NEXT_BOUNDARY("next boundary"),
        NEXT_DISTINCT_VALUE("next distinct value");

        private final String description;

        QueryKind(String description) {
            this.description = description;
        }
    }

    /**
     * Owns the discovery connection and its reusable statements. A connection failure invalidates
     * all statements, while a transient statement failure retries on the current connection. The
     * caller advances its partition cursor only after this executor returns a value, so reconnects
     * resume the query for the current boundary rather than restarting or skipping a range.
     */
    private final class DiscoveryQueryExecutor implements AutoCloseable {

        private static final long NANOS_PER_SECOND = 1_000_000_000L;

        private final JdbcConnectionProvider provider;
        private final String minQuery;
        private final String boundaryQuery;
        private final String nextValueQuery;
        private final long startedAtNanos;
        private final long totalTimeoutNanos;

        private PreparedStatement minStatement;
        private PreparedStatement boundaryStatement;
        private PreparedStatement nextValueStatement;

        private DiscoveryQueryExecutor(
                JdbcConnectionProvider provider,
                String minQuery,
                String boundaryQuery,
                String nextValueQuery,
                long startedAtNanos) {
            this.provider = provider;
            this.minQuery = minQuery;
            this.boundaryQuery = boundaryQuery;
            this.nextValueQuery = nextValueQuery;
            this.startedAtNanos = startedAtNanos;
            this.totalTimeoutNanos = durationToNanosSaturated(discoveryTotalTimeout);
        }

        private Object query(QueryKind kind, Serializable[] parameters)
                throws SQLException, ClassNotFoundException {
            for (int attempt = 0; ; attempt++) {
                ensureWithinTotalTimeout(null);
                try {
                    ensureStatements();
                    PreparedStatement statement = statement(kind);
                    statement.clearParameters();
                    for (int i = 0; i < parameters.length; i++) {
                        statement.setObject(i + 1, parameters[i]);
                    }
                    statement.setQueryTimeout(effectiveQueryTimeoutSeconds());

                    Object value;
                    try (ResultSet resultSet = statement.executeQuery()) {
                        if (!resultSet.next()) {
                            throw new SQLException(
                                    "No result returned by dynamic JDBC "
                                            + kind.description
                                            + " query.");
                        }
                        value = resultSet.getObject(1);
                    }
                    ensureWithinTotalTimeout(null);
                    return value;
                } catch (SQLException failure) {
                    boolean connectionFailure = isConnectionFailure(failure);
                    if ((!connectionFailure && !isTransientFailure(failure))
                            || attempt >= discoveryMaxRetries) {
                        throw failure;
                    }

                    if (connectionFailure) {
                        closeStatements();
                        provider.closeConnection();
                    }
                    LOG.warn(
                            "Dynamic JDBC {} query failed for {}; {} ({}/{}). Cause: {}",
                            kind.description,
                            quotedTable,
                            connectionFailure
                                    ? "reconnecting and retrying"
                                    : "retrying on the current connection",
                            attempt + 1,
                            discoveryMaxRetries,
                            failure.getMessage());
                    waitBeforeRetry(failure);
                }
            }
        }

        private void ensureStatements() throws SQLException, ClassNotFoundException {
            if (minStatement != null) {
                return;
            }
            Connection connection = provider.getOrEstablishConnection();
            try {
                minStatement = connection.prepareStatement(minQuery);
                boundaryStatement = connection.prepareStatement(boundaryQuery);
                nextValueStatement = connection.prepareStatement(nextValueQuery);
            } catch (SQLException failure) {
                closeStatements();
                throw failure;
            }
        }

        private PreparedStatement statement(QueryKind kind) {
            switch (kind) {
                case MINIMUM:
                    return minStatement;
                case NEXT_BOUNDARY:
                    return boundaryStatement;
                case NEXT_DISTINCT_VALUE:
                    return nextValueStatement;
                default:
                    throw new IllegalArgumentException("Unsupported discovery query: " + kind);
            }
        }

        private int effectiveQueryTimeoutSeconds() throws SQLTimeoutException {
            long remainingNanos = remainingNanos();
            if (remainingNanos <= 0) {
                throw totalTimeoutException(null);
            }
            long queryTimeoutNanos = durationToNanosSaturated(discoveryQueryTimeout);
            long effectiveNanos = Math.min(queryTimeoutNanos, remainingNanos);
            long seconds =
                    effectiveNanos / NANOS_PER_SECOND
                            + (effectiveNanos % NANOS_PER_SECOND == 0 ? 0 : 1);
            return (int) Math.min(Integer.MAX_VALUE, Math.max(1, seconds));
        }

        private void waitBeforeRetry(SQLException failure) throws SQLException {
            ensureWithinTotalTimeout(failure);
            long backoffNanos = durationToNanosSaturated(discoveryRetryBackoff);
            if (backoffNanos == 0) {
                return;
            }
            long sleepNanos = Math.min(backoffNanos, remainingNanos());
            try {
                long millis = sleepNanos / 1_000_000;
                int nanos = (int) (sleepNanos % 1_000_000);
                Thread.sleep(millis, nanos);
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                SQLException exception =
                        new SQLException(
                                "Interrupted while retrying JDBC partition discovery.",
                                interrupted);
                exception.addSuppressed(failure);
                throw exception;
            }
            ensureWithinTotalTimeout(failure);
        }

        private long remainingNanos() {
            return totalTimeoutNanos - (System.nanoTime() - startedAtNanos);
        }

        private void ensureWithinTotalTimeout(SQLException failure) throws SQLTimeoutException {
            if (remainingNanos() <= 0) {
                throw totalTimeoutException(failure);
            }
        }

        private SQLTimeoutException totalTimeoutException(SQLException failure) {
            SQLTimeoutException timeout =
                    new SQLTimeoutException(
                            "Dynamic JDBC partition discovery exceeded "
                                    + "scan.partition.discovery.total-timeout="
                                    + discoveryTotalTimeout
                                    + " for "
                                    + quotedTable
                                    + ".");
            if (failure != null) {
                timeout.initCause(failure);
            }
            return timeout;
        }

        @Override
        public void close() {
            closeStatements();
        }

        private void closeStatements() {
            closeStatement(nextValueStatement);
            closeStatement(boundaryStatement);
            closeStatement(minStatement);
            nextValueStatement = null;
            boundaryStatement = null;
            minStatement = null;
        }

        private void closeStatement(PreparedStatement statement) {
            if (statement == null) {
                return;
            }
            try {
                statement.close();
            } catch (SQLException closeFailure) {
                LOG.debug("Failed to close a JDBC partition discovery statement.", closeFailure);
            }
        }
    }

    private static long durationToNanosSaturated(Duration duration) {
        try {
            return duration.toNanos();
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }

    private static boolean isConnectionFailure(SQLException failure) {
        return matchesSqlException(
                failure,
                exception -> {
                    String sqlState = exception.getSQLState();
                    return exception instanceof SQLRecoverableException
                            || exception instanceof SQLTransientConnectionException
                            || exception instanceof SQLNonTransientConnectionException
                            || (sqlState != null && sqlState.startsWith("08"));
                });
    }

    private static boolean isTransientFailure(SQLException failure) {
        return matchesSqlException(
                failure,
                exception -> {
                    String sqlState = exception.getSQLState();
                    return exception instanceof SQLTransientException
                            || (sqlState != null && sqlState.startsWith("40"))
                            || "HYT00".equals(sqlState)
                            || "HYT01".equals(sqlState);
                });
    }

    private static boolean matchesSqlException(
            SQLException failure, java.util.function.Predicate<SQLException> predicate) {
        Throwable cause = failure;
        while (cause != null) {
            if (cause instanceof SQLException) {
                SQLException sqlException = (SQLException) cause;
                while (sqlException != null) {
                    if (predicate.test(sqlException)) {
                        return true;
                    }
                    sqlException = sqlException.getNextException();
                }
            }
            cause = cause.getCause();
        }
        return false;
    }

    private static final class FixedConnectionProvider implements JdbcConnectionProvider {

        private static final long serialVersionUID = 1L;

        private final transient Connection connection;

        private FixedConnectionProvider(Connection connection) {
            this.connection = connection;
        }

        @Override
        public Connection getConnection() {
            return connection;
        }

        @Override
        public boolean isConnectionValid() throws SQLException {
            return connection != null && !connection.isClosed();
        }

        @Override
        public Connection getOrEstablishConnection() {
            return connection;
        }

        @Override
        public void closeConnection() {
            // The caller owns this connection.
        }

        @Override
        public Connection reestablishConnection() {
            return connection;
        }
    }

    private String buildMinQuery() {
        return "SELECT MIN(" + quotedPartitionColumn + ") FROM " + quotedTable + predicateClause();
    }

    private String buildNextBoundaryQuery() {
        return "SELECT MIN("
                + quotedPartitionColumn
                + ") FROM (SELECT "
                + quotedPartitionColumn
                + " FROM "
                + quotedTable
                + whereWithPartitionPredicate(quotedPartitionColumn + " >= ?")
                + " ORDER BY "
                + quotedPartitionColumn
                + " ASC LIMIT "
                + chunkSize
                + ", 1) AS jdbc_dynamic_chunk";
    }

    private String buildNextValueQuery() {
        return "SELECT MIN("
                + quotedPartitionColumn
                + ") FROM "
                + quotedTable
                + whereWithPartitionPredicate(quotedPartitionColumn + " > ?");
    }

    private String predicateClause() {
        if (predicates.isEmpty()) {
            return "";
        }
        return " WHERE " + joinedPredicates();
    }

    private String whereWithPartitionPredicate(String partitionPredicate) {
        if (predicates.isEmpty()) {
            return " WHERE " + partitionPredicate;
        }
        return " WHERE " + joinedPredicates() + " AND (" + partitionPredicate + ")";
    }

    private String joinedPredicates() {
        return predicates.stream()
                .map(predicate -> "(" + predicate + ")")
                .collect(Collectors.joining(" AND "));
    }

    private static Serializable asSerializable(Object value) {
        if (!(value instanceof Serializable)) {
            throw new IllegalArgumentException(
                    "Partition boundary is not serializable: " + value.getClass().getName());
        }
        return (Serializable) value;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static int compare(Object left, Object right) {
        if (!left.getClass().isInstance(right) || !(left instanceof Comparable)) {
            throw new IllegalArgumentException(
                    "Partition boundaries must have the same comparable type, but found "
                            + left.getClass().getName()
                            + " and "
                            + right.getClass().getName());
        }
        return ((Comparable) left).compareTo(right);
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof JdbcDynamicKeysetParametersProvider)) {
            return false;
        }
        JdbcDynamicKeysetParametersProvider that = (JdbcDynamicKeysetParametersProvider) o;
        return chunkSize == that.chunkSize
                && maxSplits == that.maxSplits
                && discoveryMaxRetries == that.discoveryMaxRetries
                && Objects.equals(connectionOptions, that.connectionOptions)
                && Objects.equals(quotedTable, that.quotedTable)
                && Objects.equals(quotedPartitionColumn, that.quotedPartitionColumn)
                && Objects.equals(discoveryRetryBackoff, that.discoveryRetryBackoff)
                && Objects.equals(discoveryQueryTimeout, that.discoveryQueryTimeout)
                && Objects.equals(discoveryTotalTimeout, that.discoveryTotalTimeout)
                && Objects.equals(predicates, that.predicates)
                && java.util.Arrays.equals(predicateParameters, that.predicateParameters);
    }

    @Override
    public int hashCode() {
        return 31
                        * Objects.hash(
                                connectionOptions,
                                quotedTable,
                                quotedPartitionColumn,
                                chunkSize,
                                maxSplits,
                                discoveryMaxRetries,
                                discoveryRetryBackoff,
                                discoveryQueryTimeout,
                                discoveryTotalTimeout,
                                predicates)
                + java.util.Arrays.hashCode(predicateParameters);
    }
}
