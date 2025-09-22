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

package org.apache.flink.cdc.connectors.postgres.table;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.postgres.source.config.PostgresSourceOptions;
import org.apache.flink.cdc.connectors.postgres.utils.OptionUtils;
import org.apache.flink.cdc.debezium.table.DebeziumChangelogMode;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** Factory for creating configured instance of {@link PostgreSQLTableSource}. */
public class PostgreSQLTableFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "postgres-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(DynamicTableFactory.Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String hostname = config.get(JdbcSourceOptions.HOSTNAME);
        String username = config.get(JdbcSourceOptions.USERNAME);
        String password = config.get(JdbcSourceOptions.PASSWORD);
        String databaseName = config.get(JdbcSourceOptions.DATABASE_NAME);
        String schemaName = config.get(JdbcSourceOptions.SCHEMA_NAME);
        String tableName = config.get(JdbcSourceOptions.TABLE_NAME);
        int port = config.get(PostgresSourceOptions.PG_PORT);
        String pluginName = config.get(PostgresSourceOptions.DECODING_PLUGIN_NAME);
        String slotName = config.get(PostgresSourceOptions.SLOT_NAME);
        DebeziumChangelogMode changelogMode = config.get(PostgresSourceOptions.CHANGELOG_MODE);
        ResolvedSchema physicalSchema =
                ResolvedSchemaUtils.getPhysicalSchema(
                        context.getCatalogTable().getResolvedSchema());
        if (changelogMode == DebeziumChangelogMode.UPSERT) {
            Preconditions.checkArgument(
                    physicalSchema.getPrimaryKey().isPresent(),
                    "Primary key must be present when upsert mode is selected.");
        }
        StartupOptions startupOptions = getStartupOptions(config);
        int splitSize = config.get(PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(PostgresSourceOptions.CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(PostgresSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        Duration connectTimeout = config.get(PostgresSourceOptions.CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(PostgresSourceOptions.CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(PostgresSourceOptions.CONNECTION_POOL_SIZE);
        double distributionFactorUpper =
                config.get(PostgresSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower =
                config.get(PostgresSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        Duration heartbeatInterval = config.get(PostgresSourceOptions.HEARTBEAT_INTERVAL);
        String chunkKeyColumn =
                config.getOptional(PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN)
                        .orElse(null);

        boolean closeIdlerReaders =
                config.get(SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill =
                config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean isScanNewlyAddedTableEnabled =
                config.get(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        int lsnCommitCheckpointsDelay =
                config.get(PostgresSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY);
        boolean includePartitionedTables =
                config.get(PostgresSourceOptions.SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED);
        boolean assignUnboundedChunkFirst =
                config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        boolean appendOnly = config.get(SourceOptions.SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED);

        validateIntegerOption(
                PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(PostgresSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(PostgresSourceOptions.CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(JdbcSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(JdbcSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        OptionUtils.printOptions(IDENTIFIER, config.toMap());

        return new PostgreSQLTableSource(
                physicalSchema,
                port,
                hostname,
                databaseName,
                schemaName,
                tableName,
                username,
                password,
                pluginName,
                slotName,
                changelogMode,
                DebeziumOptions.getDebeziumProperties(context.getCatalogTable().getOptions()),
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                heartbeatInterval,
                startupOptions,
                chunkKeyColumn,
                closeIdlerReaders,
                skipSnapshotBackfill,
                isScanNewlyAddedTableEnabled,
                lsnCommitCheckpointsDelay,
                assignUnboundedChunkFirst,
                appendOnly,
                includePartitionedTables);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(JdbcSourceOptions.HOSTNAME);
        options.add(JdbcSourceOptions.USERNAME);
        options.add(JdbcSourceOptions.PASSWORD);
        options.add(JdbcSourceOptions.DATABASE_NAME);
        options.add(JdbcSourceOptions.SCHEMA_NAME);
        options.add(JdbcSourceOptions.TABLE_NAME);
        options.add(PostgresSourceOptions.SLOT_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(PostgresSourceOptions.PG_PORT);
        options.add(PostgresSourceOptions.DECODING_PLUGIN_NAME);
        options.add(PostgresSourceOptions.CHANGELOG_MODE);
        options.add(PostgresSourceOptions.SCAN_STARTUP_MODE);
        options.add(PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(PostgresSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(PostgresSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(PostgresSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(PostgresSourceOptions.CHUNK_META_GROUP_SIZE);
        options.add(PostgresSourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(PostgresSourceOptions.CONNECT_TIMEOUT);
        options.add(PostgresSourceOptions.CONNECT_MAX_RETRIES);
        options.add(PostgresSourceOptions.CONNECTION_POOL_SIZE);
        options.add(PostgresSourceOptions.HEARTBEAT_INTERVAL);
        options.add(SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(PostgresSourceOptions.SCAN_LSN_COMMIT_CHECKPOINTS_DELAY);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        options.add(SourceOptions.SCAN_READ_CHANGELOG_AS_APPEND_ONLY_ENABLED);
        options.add(PostgresSourceOptions.SCAN_INCLUDE_PARTITIONED_TABLES_ENABLED);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";

    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_COMMITTED = "committed-offset";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(PostgresSourceOptions.SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_COMMITTED:
                return StartupOptions.committed();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                PostgresSourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_COMMITTED,
                                modeString));
        }
    }

    /** Checks the value of given integer option is valid. */
    private void validateIntegerOption(
            ConfigOption<Integer> option, int optionValue, int exclusiveMin) {
        Preconditions.checkState(
                optionValue > exclusiveMin,
                String.format(
                        "The value of option '%s' must larger than %d, but is %d",
                        option.key(), exclusiveMin, optionValue));
    }

    /** Checks the value of given evenly distribution factor upper bound is valid. */
    private void validateDistributionFactorUpper(double distributionFactorUpper) {
        Preconditions.checkState(
                ObjectUtils.doubleCompare(distributionFactorUpper, 1.0d) >= 0,
                String.format(
                        "The value of option '%s' must larger than or equals %s, but is %s",
                        PostgresSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
                        1.0d,
                        distributionFactorUpper));
    }

    /** Checks the value of given evenly distribution factor lower bound is valid. */
    private void validateDistributionFactorLower(double distributionFactorLower) {
        Preconditions.checkState(
                ObjectUtils.doubleCompare(distributionFactorLower, 0.0d) >= 0
                        && ObjectUtils.doubleCompare(distributionFactorLower, 1.0d) <= 0,
                String.format(
                        "The value of option '%s' must between %s and %s inclusively, but is %s",
                        PostgresSourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }
}
