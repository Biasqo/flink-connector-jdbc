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
import org.apache.flink.connector.jdbc.core.table.sink.JdbcDynamicTableSinkITCase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.types.Row;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements ClickHouseTestBase {

    @Override
    protected ClickHouseTableRow createAppendOutputTable() {
        return ClickHouseTableBuilder.clickHouseTableRow(
                "dynamicSinkForAppend",
                ClickHouseTableBuilder.pkField("id", DataTypes.INT().notNull()),
                ClickHouseTableBuilder.field(
                        "num",
                        ClickHouseTableBuilder.dbType("Int64"),
                        DataTypes.BIGINT().notNull()),
                ClickHouseTableBuilder.field(
                        "ts",
                        ClickHouseTableBuilder.dbType("DateTime64(6)"),
                        DataTypes.TIMESTAMP()));
    }

    @Override
    protected ClickHouseTableRow createBatchOutputTable() {
        return ClickHouseTableBuilder.clickHouseTableRow(
                "dynamicSinkForBatch",
                ClickHouseTableBuilder.pkField("NAME", DataTypes.VARCHAR(20).notNull()),
                ClickHouseTableBuilder.field(
                        "SCORE",
                        ClickHouseTableBuilder.dbType("Int64"),
                        DataTypes.BIGINT().notNull()));
    }

    @Override
    protected ClickHouseTableRow createRealOutputTable() {
        return ClickHouseTableBuilder.clickHouseTableRow(
                "REAL_TABLE",
                ClickHouseTableBuilder.pkField(
                        "real_data", ClickHouseTableBuilder.dbType("Float32"), DataTypes.FLOAT()));
    }

    @Override
    protected List<Row> testUserData() {
        return Arrays.asList(
                Row.of(
                        "user1",
                        "Tom",
                        "tom123@gmail.com",
                        new BigDecimal("8.1"),
                        new BigDecimal("16.2")),
                Row.of(
                        "user3",
                        "Bailey",
                        "bailey@qq.com",
                        new BigDecimal("9.99"),
                        new BigDecimal("19.98")),
                Row.of(
                        "user4",
                        "Tina",
                        "tina@gmail.com",
                        new BigDecimal("11.3"),
                        new BigDecimal("22.6")));
    }
}
