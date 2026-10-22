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

package org.apache.flink.connector.jdbc.mysql.table;

import org.apache.flink.connector.jdbc.core.table.source.JdbcDynamicTableSource;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.utils.FactoryMocks;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Factory tests for MySQL dynamic keyset scans. */
class MySqlDynamicKeysetTableFactoryTest {

    @Test
    void createsMySqlDynamicKeysetSource() {
        DynamicTableSource source =
                FactoryMocks.createTableSource(
                        schema(DataTypes.BIGINT().notNull()), dynamicKeysetOptions());

        assertThat(source).isInstanceOf(JdbcDynamicTableSource.class);
    }

    @Test
    void rejectsNullableDynamicPartitionColumn() {
        assertThatThrownBy(
                        () ->
                                FactoryMocks.createTableSource(
                                        schema(DataTypes.BIGINT()), dynamicKeysetOptions()))
                .hasStackTraceContaining("Dynamic partition column 'id' must be declared NOT NULL");
    }

    private static ResolvedSchema schema(org.apache.flink.table.types.DataType idType) {
        return new ResolvedSchema(
                Collections.singletonList(Column.physical("id", idType)),
                Collections.emptyList(),
                null);
    }

    private static Map<String, String> dynamicKeysetOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "jdbc");
        options.put("url", "jdbc:mysql://localhost:3306/app");
        options.put("table-name", "orders");
        options.put("scan.partition.strategy", "dynamic-keyset");
        options.put("scan.partition.column", "id");
        options.put("scan.partition.chunk-size", "8096");
        return options;
    }
}
