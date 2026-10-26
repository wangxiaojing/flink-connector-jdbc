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

import org.apache.flink.connector.jdbc.datasource.connections.JdbcConnectionProvider;
import org.apache.flink.connector.jdbc.derby.database.dialect.DerbyDialect;
import org.apache.flink.connector.jdbc.internal.options.InternalJdbcConnectionOptions;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLSyntaxErrorException;
import java.sql.SQLTimeoutException;
import java.sql.SQLTransientException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link JdbcDynamicKeysetParametersProvider}. */
class JdbcDynamicKeysetParametersProviderTest {

    private static final String URL = "jdbc:h2:mem:dynamic_keyset;MODE=MySQL;DB_CLOSE_DELAY=-1";

    private Connection connection;
    private InternalJdbcConnectionOptions connectionOptions;

    @BeforeEach
    void setUp() throws Exception {
        connection = DriverManager.getConnection(URL);
        try (Statement statement = connection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS ORDERS");
            statement.execute(
                    "CREATE TABLE ORDERS (ID BIGINT NOT NULL, STATUS VARCHAR(16) NOT NULL)");
            statement.execute(
                    "INSERT INTO ORDERS VALUES "
                            + "(1, 'VALID'), (2, 'IGNORED'), "
                            + "(1000000, 'VALID'), (1000000000000, 'VALID')");
        }
        connectionOptions =
                InternalJdbcConnectionOptions.builder()
                        .setDBUrl(URL)
                        .setTableName("ORDERS")
                        .setDriverName("org.h2.Driver")
                        .setDialect(new DerbyDialect())
                        .build();
    }

    @AfterEach
    void tearDown() throws Exception {
        connection.close();
    }

    @Test
    void discoversBoundariesFromFilteredRowsInsteadOfNumericDistance() throws Exception {
        JdbcDynamicKeysetParametersProvider provider = provider(2, "VALID");

        assertThat(provider.discoverParameterValues(connection))
                .isDeepEqualTo(
                        new Serializable[][] {
                            parameters(1L, 1000000000000L, 1000000000000L, "VALID"),
                            parameters(1000000000000L, null, null, "VALID")
                        });
    }

    @Test
    void supportsSingleRowChunksWithoutLoopingOnTheSameKey() throws Exception {
        JdbcDynamicKeysetParametersProvider provider = provider(1, "VALID");

        assertThat(provider.discoverParameterValues(connection))
                .isDeepEqualTo(
                        new Serializable[][] {
                            parameters(1L, 1000000L, 1000000L, "VALID"),
                            parameters(1000000L, 1000000000000L, 1000000000000L, "VALID"),
                            parameters(1000000000000L, null, null, "VALID")
                        });
    }

    @Test
    void createsOneFinalSplitWhenAllRowsFitInOneChunk() throws Exception {
        JdbcDynamicKeysetParametersProvider provider = provider(8096, "VALID");

        assertThat(provider.discoverParameterValues(connection))
                .isDeepEqualTo(new Serializable[][] {parameters(1L, null, null, "VALID")});
    }

    @Test
    void keepsRemainingRowsInOneFinalPartialChunk() throws Exception {
        JdbcDynamicKeysetParametersProvider provider = provider(2, "ALL");

        try (Statement statement = connection.createStatement()) {
            statement.execute("UPDATE ORDERS SET STATUS = 'ALL'");
        }

        assertThat(provider.discoverParameterValues(connection))
                .isDeepEqualTo(
                        new Serializable[][] {
                            parameters(1L, 1000000L, 1000000L, "ALL"),
                            parameters(1000000L, null, null, "ALL")
                        });
    }

    @Test
    void returnsNoSplitsWhenTheFilterMatchesNoRows() throws Exception {
        JdbcDynamicKeysetParametersProvider provider = provider(8096, "MISSING");

        assertThat(provider.discoverParameterValues(connection)).isEmpty();
    }

    @Test
    void failsBeforeCreatingMoreThanTheConfiguredMaximumSplits() {
        JdbcDynamicKeysetParametersProvider provider = provider(1, 2, 0, Duration.ZERO, "VALID");

        assertThatThrownBy(() -> provider.discoverParameterValues(connection))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("scan.partition.max-splits=2");
    }

    @Test
    void reusesStatementsAppliesTimeoutAndRetriesTransientFailures() throws Exception {
        AtomicInteger prepareCalls = new AtomicInteger();
        AtomicInteger timeoutCalls = new AtomicInteger();
        AtomicBoolean failFirstQuery = new AtomicBoolean(true);
        Connection instrumentedConnection =
                (Connection)
                        Proxy.newProxyInstance(
                                getClass().getClassLoader(),
                                new Class<?>[] {Connection.class},
                                (proxy, method, args) -> {
                                    try {
                                        Object result = method.invoke(connection, args);
                                        if (!method.getName().equals("prepareStatement")) {
                                            return result;
                                        }
                                        prepareCalls.incrementAndGet();
                                        PreparedStatement delegate = (PreparedStatement) result;
                                        return Proxy.newProxyInstance(
                                                getClass().getClassLoader(),
                                                new Class<?>[] {PreparedStatement.class},
                                                (statementProxy,
                                                        statementMethod,
                                                        statementArgs) -> {
                                                    if (statementMethod
                                                            .getName()
                                                            .equals("setQueryTimeout")) {
                                                        timeoutCalls.incrementAndGet();
                                                        assertThat(statementArgs[0]).isEqualTo(2);
                                                    }
                                                    if (statementMethod
                                                                    .getName()
                                                                    .equals("executeQuery")
                                                            && failFirstQuery.getAndSet(false)) {
                                                        throw new SQLTransientException(
                                                                "transient failure");
                                                    }
                                                    try {
                                                        return statementMethod.invoke(
                                                                delegate, statementArgs);
                                                    } catch (InvocationTargetException e) {
                                                        throw e.getCause();
                                                    }
                                                });
                                    } catch (InvocationTargetException e) {
                                        throw e.getCause();
                                    }
                                });
        JdbcDynamicKeysetParametersProvider provider =
                provider(2, 100, 1, Duration.ofMillis(1500), "VALID");

        assertThat(provider.discoverParameterValues(instrumentedConnection)).hasDimensions(2, 4);
        assertThat(prepareCalls).hasValue(3);
        assertThat(timeoutCalls).hasValue(4);
        assertThat(failFirstQuery).isFalse();
    }

    @Test
    void reconnectsAndResumesFromTheCurrentBoundary() throws Exception {
        AtomicInteger connectionCount = new AtomicInteger();
        AtomicInteger firstConnectionExecutions = new AtomicInteger();
        AtomicReference<Connection> currentConnection = new AtomicReference<>();
        JdbcConnectionProvider connectionProvider =
                new JdbcConnectionProvider() {
                    @Override
                    public Connection getConnection() {
                        return currentConnection.get();
                    }

                    @Override
                    public boolean isConnectionValid() throws SQLException {
                        Connection current = currentConnection.get();
                        return current != null && !current.isClosed();
                    }

                    @Override
                    public Connection getOrEstablishConnection() throws SQLException {
                        if (isConnectionValid()) {
                            return currentConnection.get();
                        }
                        Connection delegate = DriverManager.getConnection(URL);
                        if (connectionCount.incrementAndGet() == 1) {
                            delegate =
                                    connectionFailingOnExecution(
                                            delegate, firstConnectionExecutions, 3);
                        }
                        currentConnection.set(delegate);
                        return delegate;
                    }

                    @Override
                    public void closeConnection() {
                        Connection current = currentConnection.getAndSet(null);
                        if (current != null) {
                            try {
                                current.close();
                            } catch (SQLException ignored) {
                                // Test cleanup only.
                            }
                        }
                    }

                    @Override
                    public Connection reestablishConnection() throws SQLException {
                        closeConnection();
                        return getOrEstablishConnection();
                    }
                };
        JdbcDynamicKeysetParametersProvider provider =
                provider(2, 100, 2, Duration.ofMinutes(5), "VALID");

        try {
            assertThat(provider.discoverParameterValues(connectionProvider))
                    .isDeepEqualTo(
                            new Serializable[][] {
                                parameters(1L, 1000000000000L, 1000000000000L, "VALID"),
                                parameters(1000000000000L, null, null, "VALID")
                            });
            assertThat(connectionCount).hasValue(2);
            assertThat(firstConnectionExecutions).hasValue(3);
        } finally {
            connectionProvider.closeConnection();
        }
    }

    @Test
    void doesNotRetryNonTransientSqlFailures() {
        AtomicInteger executions = new AtomicInteger();
        Connection failingConnection =
                connectionThrowingOnEveryExecution(
                        connection,
                        executions,
                        () -> new SQLSyntaxErrorException("invalid discovery query", "42000"));
        JdbcDynamicKeysetParametersProvider provider = provider(2, "VALID");

        assertThatThrownBy(() -> provider.discoverParameterValues(failingConnection))
                .isInstanceOf(SQLSyntaxErrorException.class);
        assertThat(executions).hasValue(1);
    }

    @Test
    void totalTimeoutIncludesRetryBackoff() {
        AtomicInteger executions = new AtomicInteger();
        Connection failingConnection =
                connectionThrowingOnEveryExecution(
                        connection,
                        executions,
                        () -> new SQLTransientException("retryable failure", "40001"));
        JdbcDynamicKeysetParametersProvider provider =
                provider(
                        2,
                        100,
                        10,
                        Duration.ofSeconds(1),
                        Duration.ofMinutes(5),
                        Duration.ofMillis(30),
                        "VALID");

        assertThatThrownBy(() -> provider.discoverParameterValues(failingConnection))
                .isInstanceOf(SQLTimeoutException.class)
                .hasMessageContaining("scan.partition.discovery.total-timeout=PT0.03S");
        assertThat(executions).hasValue(1);
    }

    private JdbcDynamicKeysetParametersProvider provider(int chunkSize, String status) {
        return provider(chunkSize, 100_000, 3, Duration.ofMinutes(5), status);
    }

    private JdbcDynamicKeysetParametersProvider provider(
            int chunkSize, int maxSplits, int maxRetries, Duration queryTimeout, String status) {
        return provider(
                chunkSize,
                maxSplits,
                maxRetries,
                Duration.ZERO,
                queryTimeout,
                Duration.ofMinutes(30),
                status);
    }

    private JdbcDynamicKeysetParametersProvider provider(
            int chunkSize,
            int maxSplits,
            int maxRetries,
            Duration retryBackoff,
            Duration queryTimeout,
            Duration totalTimeout,
            String status) {
        return new JdbcDynamicKeysetParametersProvider(
                connectionOptions,
                new DerbyDialect(),
                "ID",
                chunkSize,
                maxSplits,
                maxRetries,
                retryBackoff,
                queryTimeout,
                totalTimeout,
                Collections.singletonList("STATUS = ?"),
                new Serializable[] {status});
    }

    private Connection connectionFailingOnExecution(
            Connection delegate, AtomicInteger executions, int failureExecution) {
        return connectionWithStatementFailure(
                delegate,
                executions,
                () ->
                        executions.get() == failureExecution
                                ? new SQLRecoverableException("connection lost", "08006")
                                : null);
    }

    private Connection connectionThrowingOnEveryExecution(
            Connection delegate,
            AtomicInteger executions,
            java.util.function.Supplier<SQLException> failureSupplier) {
        return connectionWithStatementFailure(delegate, executions, failureSupplier);
    }

    private Connection connectionWithStatementFailure(
            Connection delegate,
            AtomicInteger executions,
            java.util.function.Supplier<SQLException> failureSupplier) {
        return (Connection)
                Proxy.newProxyInstance(
                        getClass().getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            try {
                                Object result = method.invoke(delegate, args);
                                if (!method.getName().equals("prepareStatement")) {
                                    return result;
                                }
                                PreparedStatement statement = (PreparedStatement) result;
                                return Proxy.newProxyInstance(
                                        getClass().getClassLoader(),
                                        new Class<?>[] {PreparedStatement.class},
                                        (statementProxy, statementMethod, statementArgs) -> {
                                            if (statementMethod.getName().equals("executeQuery")) {
                                                executions.incrementAndGet();
                                                SQLException failure = failureSupplier.get();
                                                if (failure != null) {
                                                    throw failure;
                                                }
                                            }
                                            try {
                                                return statementMethod.invoke(
                                                        statement, statementArgs);
                                            } catch (InvocationTargetException e) {
                                                throw e.getCause();
                                            }
                                        });
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                        });
    }

    private static Serializable[] parameters(Serializable... values) {
        return values;
    }
}
