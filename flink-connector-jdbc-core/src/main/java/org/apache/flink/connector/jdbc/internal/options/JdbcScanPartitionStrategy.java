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

package org.apache.flink.connector.jdbc.internal.options;

import java.util.Locale;

/** Partition strategies supported by JDBC bounded table scans. */
public enum JdbcScanPartitionStrategy {
    FIXED_RANGE("fixed-range"),
    DYNAMIC_KEYSET("dynamic-keyset");

    private final String optionValue;

    JdbcScanPartitionStrategy(String optionValue) {
        this.optionValue = optionValue;
    }

    public String optionValue() {
        return optionValue;
    }

    public static JdbcScanPartitionStrategy fromOption(String value) {
        String normalized = value.trim().toLowerCase(Locale.ROOT);
        for (JdbcScanPartitionStrategy strategy : values()) {
            if (strategy.optionValue.equals(normalized)) {
                return strategy;
            }
        }
        throw new IllegalArgumentException(
                "Unsupported scan.partition.strategy '"
                        + value
                        + "'. Supported values are fixed-range and dynamic-keyset.");
    }
}
