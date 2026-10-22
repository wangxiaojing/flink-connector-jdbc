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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/** Discovers keyset boundaries from actual rows for sparse MySQL partition keys. */
final class JdbcDynamicKeysetParametersProvider implements JdbcParameterValuesProvider {

    private static final long serialVersionUID = 1L;

    private final InternalJdbcConnectionOptions connectionOptions;
    private final String quotedTable;
    private final String quotedPartitionColumn;
    private final int chunkSize;
    private final List<String> predicates;
    private final Serializable[] predicateParameters;

    JdbcDynamicKeysetParametersProvider(
            InternalJdbcConnectionOptions connectionOptions,
            JdbcDialect dialect,
            String partitionColumn,
            int chunkSize,
            List<String> predicates,
            Serializable[] predicateParameters) {
        this.connectionOptions = connectionOptions;
        this.quotedTable = dialect.quoteIdentifier(connectionOptions.getTableName());
        this.quotedPartitionColumn = dialect.quoteIdentifier(partitionColumn);
        this.chunkSize = chunkSize;
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
        Object min = querySingleValue(connection, buildMinQuery(), predicateParameters);
        if (min == null) {
            return new Serializable[0][];
        }

        List<Serializable[]> partitions = new ArrayList<>();
        Object start = min;
        while (true) {
            Object end = queryNextBoundary(connection, start);
            if (end == null || compare(start, end) >= 0) {
                end = queryNextValue(connection, start);
            }
            if (end == null) {
                partitions.add(dataQueryParameters(start, null));
                break;
            }
            partitions.add(dataQueryParameters(start, end));
            start = end;
        }
        return partitions.toArray(new Serializable[0][]);
    }

    private Object queryNextBoundary(Connection connection, Object start) throws SQLException {
        Serializable[] parameters = ArrayUtils.add(predicateParameters, asSerializable(start));
        return querySingleValue(connection, buildNextBoundaryQuery(), parameters);
    }

    private Object queryNextValue(Connection connection, Object start) throws SQLException {
        Serializable[] parameters = ArrayUtils.add(predicateParameters, asSerializable(start));
        return querySingleValue(connection, buildNextValueQuery(), parameters);
    }

    private Serializable[] dataQueryParameters(Object start, Object end) {
        Serializable serializableStart = asSerializable(start);
        Serializable serializableEnd = end == null ? null : asSerializable(end);
        return ArrayUtils.addAll(
                new Serializable[] {serializableStart, serializableEnd, serializableEnd},
                predicateParameters);
    }

    private Object querySingleValue(Connection connection, String sql, Serializable[] parameters)
            throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
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
                && Objects.equals(connectionOptions, that.connectionOptions)
                && Objects.equals(quotedTable, that.quotedTable)
                && Objects.equals(quotedPartitionColumn, that.quotedPartitionColumn)
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
                                predicates)
                + java.util.Arrays.hashCode(predicateParameters);
    }
}
