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

package org.apache.flink.connector.jdbc.clickhouse.database.dialect;

import org.apache.flink.connector.jdbc.core.database.JdbcFactoryLoader;
import org.apache.flink.connector.jdbc.core.database.dialect.JdbcDialect;
import org.apache.flink.connector.jdbc.statement.FieldNamedPreparedStatementImpl;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ClickHousePreparedStatementTest}. */
class ClickHousePreparedStatementTest {

    private final JdbcDialect dialect =
            JdbcFactoryLoader.loadDialect(
                    "jdbc:clickhouse://localhost:8123/default", getClass().getClassLoader());
    private final String[] fieldNames =
            new String[] {"id", "name", "email", "ts", "field1", "field_2", "__field_3__"};
    private final String[] keyFields = new String[] {"id", "__field_3__"};
    private final String tableName = "tbl";

    @Test
    void testInsertStatement() {
        String insertStmt = dialect.getInsertIntoStatement(tableName, fieldNames);
        assertThat(insertStmt)
                .isEqualTo(
                        "INSERT INTO tbl(id, name, email, ts, field1, field_2, __field_3__) "
                                + "VALUES (:id, :name, :email, :ts, :field1, :field_2, :__field_3__)");
        NamedStatementMatcher.parsedSql(
                        "INSERT INTO tbl(id, name, email, ts, field1, field_2, __field_3__) "
                                + "VALUES (?, ?, ?, ?, ?, ?, ?)")
                .parameter("id", singletonList(1))
                .parameter("name", singletonList(2))
                .parameter("email", singletonList(3))
                .parameter("ts", singletonList(4))
                .parameter("field1", singletonList(5))
                .parameter("field_2", singletonList(6))
                .parameter("__field_3__", singletonList(7))
                .matches(insertStmt);
    }

    @Test
    void testDeleteStatement() {
        String deleteStmt = dialect.getDeleteStatement(tableName, keyFields);
        assertThat(deleteStmt)
                .isEqualTo("DELETE FROM tbl WHERE id = :id AND __field_3__ = :__field_3__");
        NamedStatementMatcher.parsedSql("DELETE FROM tbl WHERE id = ? AND __field_3__ = ?")
                .parameter("id", singletonList(1))
                .parameter("__field_3__", singletonList(2))
                .matches(deleteStmt);
    }

    @Test
    void testRowExistsStatement() {
        String rowExistStmt = dialect.getRowExistsStatement(tableName, keyFields);
        assertThat(rowExistStmt)
                .isEqualTo("SELECT 1 FROM tbl WHERE id = :id AND __field_3__ = :__field_3__");
        NamedStatementMatcher.parsedSql("SELECT 1 FROM tbl WHERE id = ? AND __field_3__ = ?")
                .parameter("id", singletonList(1))
                .parameter("__field_3__", singletonList(2))
                .matches(rowExistStmt);
    }

    @Test
    void testUpdateStatement() {
        String updateStmt = dialect.getUpdateStatement(tableName, fieldNames, keyFields);
        assertThat(updateStmt)
                .isEqualTo(
                        "ALTER TABLE tbl UPDATE name = :name, email = :email, ts = :ts, "
                                + "field1 = :field1, field_2 = :field_2 "
                                + "WHERE id = :id AND __field_3__ = :__field_3__");
        NamedStatementMatcher.parsedSql(
                        "ALTER TABLE tbl UPDATE name = ?, email = ?, ts = ?, field1 = ?, "
                                + "field_2 = ? WHERE id = ? AND __field_3__ = ?")
                .parameter("name", singletonList(1))
                .parameter("email", singletonList(2))
                .parameter("ts", singletonList(3))
                .parameter("field1", singletonList(4))
                .parameter("field_2", singletonList(5))
                .parameter("id", singletonList(6))
                .parameter("__field_3__", singletonList(7))
                .matches(updateStmt);
    }

    @Test
    void testUpsertStatement() {
        Optional<String> upsertStmt = dialect.getUpsertStatement(tableName, fieldNames, keyFields);
        assertThat(upsertStmt).isEqualTo(Optional.empty());
    }

    @Test
    void testSelectStatement() {
        String selectStmt = dialect.getSelectFromStatement(tableName, fieldNames, keyFields);
        assertThat(selectStmt)
                .isEqualTo(
                        "SELECT id, name, email, ts, field1, field_2, __field_3__ FROM tbl "
                                + "WHERE id = :id AND __field_3__ = :__field_3__");
        NamedStatementMatcher.parsedSql(
                        "SELECT id, name, email, ts, field1, field_2, __field_3__ FROM tbl "
                                + "WHERE id = ? AND __field_3__ = ?")
                .parameter("id", singletonList(1))
                .parameter("__field_3__", singletonList(2))
                .matches(selectStmt);
    }

    private static class NamedStatementMatcher {
        private String parsedSql;
        private Map<String, List<Integer>> parameterMap = new HashMap<>();

        public static NamedStatementMatcher parsedSql(String parsedSql) {
            NamedStatementMatcher spec = new NamedStatementMatcher();
            spec.parsedSql = parsedSql;
            return spec;
        }

        public NamedStatementMatcher parameter(String name, List<Integer> index) {
            this.parameterMap.put(name, index);
            return this;
        }

        public void matches(String statement) {
            Map<String, List<Integer>> actualParams = new HashMap<>();
            String actualParsedStmt =
                    FieldNamedPreparedStatementImpl.parseNamedStatement(statement, actualParams);
            assertThat(actualParsedStmt).isEqualTo(parsedSql);
            assertThat(actualParams).isEqualTo(parameterMap);
        }
    }
}
