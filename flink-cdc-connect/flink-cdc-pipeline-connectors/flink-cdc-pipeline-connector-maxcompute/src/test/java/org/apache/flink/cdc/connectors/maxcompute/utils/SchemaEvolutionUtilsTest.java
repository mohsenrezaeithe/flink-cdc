/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.maxcompute.utils;

import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataTypes;
import org.apache.flink.cdc.connectors.maxcompute.EmulatorTestBase;

import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava33.com.google.common.collect.ImmutableMap;

import com.aliyun.odps.OdpsException;
import com.aliyun.odps.TableSchema;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * e2e test of SchemaEvolutionUtils, Note that the Emulator only supports uppercase input (However,
 * MaxCompute can correctly distinguish between uppercase and lowercase).
 *
 * <p>Since the emulator does not support alter column type, the test cases here are mainly for
 * testing other schema evolution logic.
 */
class SchemaEvolutionUtilsTest extends EmulatorTestBase {

    private static final String TEST_TABLE = "SCHEMA_EVOLUTION_TEST_TABLE";

    @BeforeEach
    void testCreateTable() throws Exception {
        SchemaEvolutionUtils.createTable(
                testOptions,
                TableId.tableId(TEST_TABLE),
                Schema.newBuilder()
                        .physicalColumn("PK", DataTypes.BIGINT())
                        .physicalColumn("ID1", DataTypes.BIGINT())
                        .physicalColumn("ID2", DataTypes.BIGINT())
                        .primaryKey("PK")
                        .build());
        Assertions.assertThat(odpsInstance.tables().get(TEST_TABLE).getPrimaryKey())
                .isEqualTo(ImmutableList.of("PK"));
    }

    @AfterEach
    void deleteTable() throws OdpsException {
        odpsInstance.tables().delete(TEST_TABLE, true);
    }

    @Test
    void testAddColumn() throws Exception {
        SchemaEvolutionUtils.addColumns(
                testOptions,
                TableId.tableId(TEST_TABLE),
                ImmutableList.of(
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("ID3", DataTypes.BIGINT())),
                        new AddColumnEvent.ColumnWithPosition(
                                Column.physicalColumn("NAME", DataTypes.STRING()))));
        TableSchema schema = odpsInstance.tables().get(TEST_TABLE).getSchema();

        Assertions.assertThat(schema.getColumns()).hasSize(5);
        Assertions.assertThat(schema.getColumns().get(0).getName()).isEqualTo("PK");
        Assertions.assertThat(schema.getColumns().get(1).getName()).isEqualTo("ID1");
        Assertions.assertThat(schema.getColumns().get(2).getName()).isEqualTo("ID2");
        Assertions.assertThat(schema.getColumns().get(3).getName()).isEqualTo("ID3");
        Assertions.assertThat(schema.getColumns().get(4).getName()).isEqualTo("NAME");
    }

    @Test
    void testDropColumn() throws Exception {
        SchemaEvolutionUtils.dropColumn(
                testOptions, TableId.tableId(TEST_TABLE), ImmutableList.of("ID1", "ID2"));
        TableSchema schema = odpsInstance.tables().get(TEST_TABLE).getSchema();

        Assertions.assertThat(schema.getColumns()).hasSize(1);
        Assertions.assertThat(schema.getColumns().get(0).getName()).isEqualTo("PK");
    }

    @Test
    void testRenameColumn() throws Exception {
        TableSchema originSchema = odpsInstance.tables().get(TEST_TABLE).getSchema();
        Assertions.assertThat(originSchema.getColumns().get(1).getName()).isEqualTo("ID1");
        Assertions.assertThat(originSchema.getColumns().get(2).getName()).isEqualTo("ID2");

        SchemaEvolutionUtils.renameColumn(
                testOptions,
                TableId.tableId(TEST_TABLE),
                ImmutableMap.of("ID1", "ID1_NEW", "ID2", "ID2_NEW"));

        TableSchema expectSchema = odpsInstance.tables().get(TEST_TABLE).getSchema();
        Assertions.assertThat(expectSchema.getColumns().get(1).getName()).isEqualTo("ID1_NEW");
        Assertions.assertThat(expectSchema.getColumns().get(2).getName()).isEqualTo("ID2_NEW");
    }
}
