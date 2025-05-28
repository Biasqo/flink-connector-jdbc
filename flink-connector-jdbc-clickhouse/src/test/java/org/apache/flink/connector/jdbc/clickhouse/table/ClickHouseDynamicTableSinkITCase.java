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

import static org.apache.flink.connector.jdbc.clickhouse.table.ClickHouseTableBuilder.clickHouseTableRow;
import static org.apache.flink.connector.jdbc.clickhouse.table.ClickHouseTableBuilder.dbType;
import static org.apache.flink.connector.jdbc.clickhouse.table.ClickHouseTableBuilder.field;
import static org.apache.flink.connector.jdbc.clickhouse.table.ClickHouseTableBuilder.pkField;

/** The Table Sink ITCase for {@link ClickHouseDialect}. */
class ClickHouseDynamicTableSinkITCase extends JdbcDynamicTableSinkITCase
        implements ClickHouseTestBase {

    // ClickHouse does not support upsert statements
    @Override
    protected ClickHouseTableRow createUpsertOutputTable() {
        return clickHouseTableRow(
                "dynamicSinkForUpsert",
                pkField("cnt", DataTypes.BIGINT().notNull()),
                field("lencnt", DataTypes.BIGINT().notNull()),
                pkField("cTag", DataTypes.INT().notNull()),
                field("ts", dbType("TIMESTAMP"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected ClickHouseTableRow createAppendOutputTable() {
        return clickHouseTableRow(
                "dynamicSinkForAppend",
                pkField("id", DataTypes.INT().notNull()),
                field("num", dbType("Int64"), DataTypes.BIGINT().notNull()),
                field("ts", ClickHouseTableBuilder.dbType("DateTime64(6)"), DataTypes.TIMESTAMP()));
    }

    @Override
    protected ClickHouseTableRow createBatchOutputTable() {
        return clickHouseTableRow(
                "dynamicSinkForBatch",
                pkField("NAME", DataTypes.VARCHAR(20).notNull()),
                field("SCORE", dbType("Int64"), DataTypes.BIGINT().notNull()));
    }

    @Override
    protected ClickHouseTableRow createRealOutputTable() {
        return clickHouseTableRow(
                "REAL_TABLE", pkField("real_data", dbType("Float32"), DataTypes.FLOAT()));
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
