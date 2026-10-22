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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

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
    void returnsNoSplitsWhenTheFilterMatchesNoRows() throws Exception {
        JdbcDynamicKeysetParametersProvider provider = provider(8096, "MISSING");

        assertThat(provider.discoverParameterValues(connection)).isEmpty();
    }

    private JdbcDynamicKeysetParametersProvider provider(int chunkSize, String status) {
        return new JdbcDynamicKeysetParametersProvider(
                connectionOptions,
                new DerbyDialect(),
                "ID",
                chunkSize,
                Collections.singletonList("STATUS = ?"),
                new Serializable[] {status});
    }

    private static Serializable[] parameters(Serializable... values) {
        return values;
    }
}
