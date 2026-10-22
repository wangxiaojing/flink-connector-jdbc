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
        this.predicates = new ArrayList<>(predicates);
        this.predicateParameters = predicateParameters.clone();
    }

    @Override
    public Serializable[][] getParameterValues() {
        JdbcConnectionProvider provider = new SimpleJdbcConnectionProvider(connectionOptions);
        try {
            return discoverParameterValues(provider.getOrEstablishConnection());
        } catch (SQLException | ClassNotFoundException e) {
            throw new IllegalStateException("Failed to discover dynamic JDBC partitions.", e);
        } finally {
            provider.closeConnection();
        }
    }

    Serializable[][] discoverParameterValues(Connection connection) throws SQLException {
        long startedAtNanos = System.nanoTime();
        String minQuery = buildMinQuery();
        String boundaryQuery = buildNextBoundaryQuery();
        String nextValueQuery = buildNextValueQuery();
        try (PreparedStatement minStatement = prepareDiscoveryStatement(connection, minQuery);
                PreparedStatement boundaryStatement =
                        prepareDiscoveryStatement(connection, boundaryQuery);
                PreparedStatement nextValueStatement =
                        prepareDiscoveryStatement(connection, nextValueQuery)) {
            Object min = querySingleValue(minStatement, "minimum", minQuery, predicateParameters);
            if (min == null) {
                LOG.info(
                        "Dynamic JDBC partition discovery found no matching rows in {}.",
                        quotedTable);
                return new Serializable[0][];
            }

            List<Serializable[]> partitions = new ArrayList<>();
            Object start = min;
            while (true) {
                Object end = queryNextBoundary(boundaryStatement, boundaryQuery, start);
                if (end == null) {
                    addPartition(partitions, dataQueryParameters(start, null), startedAtNanos);
                    break;
                }
                if (compare(start, end) >= 0) {
                    end = queryNextValue(nextValueStatement, nextValueQuery, start);
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

    private Object queryNextBoundary(PreparedStatement statement, String sql, Object start)
            throws SQLException {
        Serializable[] parameters = ArrayUtils.add(predicateParameters, asSerializable(start));
        return querySingleValue(statement, "next boundary", sql, parameters);
    }

    private Object queryNextValue(PreparedStatement statement, String sql, Object start)
            throws SQLException {
        Serializable[] parameters = ArrayUtils.add(predicateParameters, asSerializable(start));
        return querySingleValue(statement, "next distinct value", sql, parameters);
    }

    private Serializable[] dataQueryParameters(Object start, Object end) {
        Serializable serializableStart = asSerializable(start);
        Serializable serializableEnd = end == null ? null : asSerializable(end);
        return ArrayUtils.addAll(
                new Serializable[] {serializableStart, serializableEnd, serializableEnd},
                predicateParameters);
    }

    private PreparedStatement prepareDiscoveryStatement(Connection connection, String sql)
            throws SQLException {
        PreparedStatement statement = connection.prepareStatement(sql);
        try {
            statement.setQueryTimeout(queryTimeoutSeconds());
            return statement;
        } catch (SQLException e) {
            statement.close();
            throw e;
        }
    }

    private Object querySingleValue(
            PreparedStatement statement, String queryName, String sql, Serializable[] parameters)
            throws SQLException {
        for (int attempt = 0; ; attempt++) {
            try {
                statement.clearParameters();
                for (int i = 0; i < parameters.length; i++) {
                    statement.setObject(i + 1, parameters[i]);
                }
                try (ResultSet resultSet = statement.executeQuery()) {
                    if (!resultSet.next()) {
                        throw new SQLException(
                                "No result returned by partition discovery query: " + sql);
                    }
                    return resultSet.getObject(1);
                }
            } catch (SQLException failure) {
                if (attempt >= discoveryMaxRetries) {
                    throw failure;
                }
                LOG.warn(
                        "Dynamic JDBC {} query failed for {}; retrying ({}/{}). Cause: {}",
                        queryName,
                        quotedTable,
                        attempt + 1,
                        discoveryMaxRetries,
                        failure.getMessage());
                waitBeforeRetry(failure);
            }
        }
    }

    private void waitBeforeRetry(SQLException failure) throws SQLException {
        if (discoveryRetryBackoff.isZero()) {
            return;
        }
        try {
            Thread.sleep(discoveryRetryBackoff.toMillis());
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            SQLException exception =
                    new SQLException(
                            "Interrupted while retrying JDBC partition discovery.", interrupted);
            exception.addSuppressed(failure);
            throw exception;
        }
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

    private int queryTimeoutSeconds() {
        long millis = discoveryQueryTimeout.toMillis();
        long seconds = millis / 1_000 + (millis % 1_000 == 0 ? 0 : 1);
        return (int) Math.min(Integer.MAX_VALUE, Math.max(1, seconds));
    }

    private static long elapsedMillis(long startedAtNanos) {
        return (System.nanoTime() - startedAtNanos) / 1_000_000;
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
                                predicates)
                + java.util.Arrays.hashCode(predicateParameters);
    }
}
