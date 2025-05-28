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

package org.apache.flink.connector.jdbc.clickhouse.table;

import org.apache.flink.connector.jdbc.clickhouse.ClickHouseTestBase;
import org.apache.flink.connector.jdbc.clickhouse.database.dialect.ClickHouseDialect;
import org.apache.flink.connector.jdbc.core.table.source.JdbcDynamicTableSourceITCase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;

/** The Table Source ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSourceITCase extends JdbcDynamicTableSourceITCase
        implements ClickHouseTestBase {

    @Override
    protected ClickHouseTableRow createInputTable() {
        return ClickHouseTableBuilder.clickHouseTableRow(
                "jdbDynamicTableSource",
                ClickHouseTableBuilder.pkField(
                        "id", ClickHouseTableBuilder.dbType("Int64"), DataTypes.BIGINT().notNull()),
                ClickHouseTableBuilder.field(
                        "decimal_col",
                        ClickHouseTableBuilder.dbType("Decimal(10,4)"),
                        DataTypes.DECIMAL(10, 4)),
                ClickHouseTableBuilder.field(
                        "timestamp6_col",
                        ClickHouseTableBuilder.dbType("DateTime64(6)"),
                        DataTypes.TIMESTAMP(6)),
                // other fields
                ClickHouseTableBuilder.field(
                        "float_col", ClickHouseTableBuilder.dbType("Float32"), DataTypes.FLOAT()),
                ClickHouseTableBuilder.field(
                        "double_col", ClickHouseTableBuilder.dbType("Float64"), DataTypes.DOUBLE()),
                ClickHouseTableBuilder.field(
                        "binary_float_col",
                        ClickHouseTableBuilder.dbType("Float32"),
                        DataTypes.FLOAT()),
                ClickHouseTableBuilder.field(
                        "binary_double_col",
                        ClickHouseTableBuilder.dbType("Float64"),
                        DataTypes.DOUBLE()),
                ClickHouseTableBuilder.field(
                        "char_col", ClickHouseTableBuilder.dbType("String"), DataTypes.CHAR(1)),
                ClickHouseTableBuilder.field(
                        "string_col",
                        ClickHouseTableBuilder.dbType("String"),
                        DataTypes.VARCHAR(3)),
                ClickHouseTableBuilder.field(
                        "string2_col",
                        ClickHouseTableBuilder.dbType("String"),
                        DataTypes.VARCHAR(30)),
                ClickHouseTableBuilder.field(
                        "date_col", ClickHouseTableBuilder.dbType("Date"), DataTypes.DATE()),
                ClickHouseTableBuilder.field(
                        "dt9_col",
                        ClickHouseTableBuilder.dbType("DateTime64(9)"),
                        DataTypes.TIMESTAMP(9)),
                ClickHouseTableBuilder.field(
                        "clob_col", ClickHouseTableBuilder.dbType("String"), DataTypes.STRING()));
    }

    @Override
    protected List<Row> getTestData() {
        return Arrays.asList(
                Row.of(
                        1L,
                        BigDecimal.valueOf(100.1234),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456"),
                        1.12345,
                        2.1234567879,
                        1.175E-10,
                        1.79769E+40,
                        "a",
                        "abc",
                        "abcdef",
                        LocalDate.parse("1997-01-01"),
                        LocalDateTime.parse("2020-01-01T15:35:00.123456789"),
                        "Hello World"),
                Row.of(
                        2L,
                        BigDecimal.valueOf(101.1234),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456"),
                        1.12345,
                        2.12345678790,
                        1.175E-10,
                        1.79769E+40,
                        "a",
                        "abc",
                        "abcdef",
                        LocalDate.parse("1997-01-02"),
                        LocalDateTime.parse("2020-01-01T15:36:01.123456789"),
                        "Hey Leonard"));
    }
}
