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
import java.sql.Statement;
import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
                                                        throw new SQLException("transient failure");
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
        assertThat(timeoutCalls).hasValue(3);
        assertThat(failFirstQuery).isFalse();
    }

    private JdbcDynamicKeysetParametersProvider provider(int chunkSize, String status) {
        return provider(chunkSize, 100_000, 3, Duration.ofMinutes(5), status);
    }

    private JdbcDynamicKeysetParametersProvider provider(
            int chunkSize, int maxSplits, int maxRetries, Duration queryTimeout, String status) {
        return new JdbcDynamicKeysetParametersProvider(
                connectionOptions,
                new DerbyDialect(),
                "ID",
                chunkSize,
                maxSplits,
                maxRetries,
                Duration.ZERO,
                queryTimeout,
                Collections.singletonList("STATUS = ?"),
                new Serializable[] {status});
    }

    private static Serializable[] parameters(Serializable... values) {
        return values;
    }
}
