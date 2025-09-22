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

package org.apache.flink.cdc.connectors.oracle.table;

import org.apache.flink.cdc.connectors.base.options.JdbcSourceOptions;
import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.utils.ObjectUtils;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.cdc.connectors.oracle.source.config.OracleSourceOptions;
import org.apache.flink.cdc.debezium.table.DebeziumOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/** Factory for creating configured instance of {@link OracleTableSource}. */
public class OracleTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "oracle-cdc";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validateExcept(DebeziumOptions.DEBEZIUM_OPTIONS_PREFIX);

        final ReadableConfig config = helper.getOptions();
        String url = config.get(OracleSourceOptions.URL);
        String hostname = config.get(JdbcSourceOptions.HOSTNAME);
        String username = config.get(JdbcSourceOptions.USERNAME);
        String password = config.get(JdbcSourceOptions.PASSWORD);
        String databaseName = config.get(JdbcSourceOptions.DATABASE_NAME);
        Preconditions.checkNotNull(databaseName);
        // During the incremental phase, Debezium uses the uppercase database name.
        // However, during the snapshot phase, the database name is user-configurable.
        // To avoid inconsistencies between the database names in the snapshot and incremental
        // phases,
        // it is necessary to convert the database name to uppercase when constructing the Oracle
        // Source.
        // For more details, please refer to:
        // https://github.com/ververica/flink-cdc-connectors/pull/2088.
        databaseName = databaseName.toUpperCase();
        String tableName = config.get(JdbcSourceOptions.TABLE_NAME);
        String schemaName = config.get(OracleSourceOptions.SCHEMA_NAME);
        int port = config.get(OracleSourceOptions.PORT);
        StartupOptions startupOptions = getStartupOptions(config);
        ResolvedSchema physicalSchema = context.getCatalogTable().getResolvedSchema();

        int splitSize = config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        int splitMetaGroupSize = config.get(SourceOptions.CHUNK_META_GROUP_SIZE);
        int fetchSize = config.get(SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        Duration connectTimeout = config.get(JdbcSourceOptions.CONNECT_TIMEOUT);
        int connectMaxRetries = config.get(JdbcSourceOptions.CONNECT_MAX_RETRIES);
        int connectionPoolSize = config.get(JdbcSourceOptions.CONNECTION_POOL_SIZE);
        double distributionFactorUpper =
                config.get(SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        double distributionFactorLower =
                config.get(SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        String chunkKeyColumn =
                config.getOptional(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN)
                        .orElse(null);

        boolean closeIdlerReaders =
                config.get(JdbcSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill =
                config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean scanNewlyAddedTableEnabled =
                config.get(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        boolean assignUnboundedChunkFirst =
                config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);

        validateIntegerOption(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE, splitSize, 1);
        validateIntegerOption(SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE, fetchSize, 1);
        validateIntegerOption(SourceOptions.CHUNK_META_GROUP_SIZE, splitMetaGroupSize, 1);
        validateIntegerOption(JdbcSourceOptions.CONNECTION_POOL_SIZE, connectionPoolSize, 1);
        validateIntegerOption(JdbcSourceOptions.CONNECT_MAX_RETRIES, connectMaxRetries, 0);
        validateDistributionFactorUpper(distributionFactorUpper);
        validateDistributionFactorLower(distributionFactorLower);

        OptionUtils.printOptions(IDENTIFIER, config.toMap());

        return new OracleTableSource(
                physicalSchema,
                url,
                port,
                hostname,
                databaseName,
                tableName,
                schemaName,
                username,
                password,
                DebeziumOptions.getDebeziumProperties(context.getCatalogTable().getOptions()),
                startupOptions,
                splitSize,
                splitMetaGroupSize,
                fetchSize,
                connectTimeout,
                connectMaxRetries,
                connectionPoolSize,
                distributionFactorUpper,
                distributionFactorLower,
                chunkKeyColumn,
                closeIdlerReaders,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(JdbcSourceOptions.USERNAME);
        options.add(JdbcSourceOptions.PASSWORD);
        options.add(JdbcSourceOptions.DATABASE_NAME);
        options.add(JdbcSourceOptions.TABLE_NAME);
        options.add(OracleSourceOptions.SCHEMA_NAME);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(OracleSourceOptions.URL);
        options.add(JdbcSourceOptions.HOSTNAME);
        options.add(OracleSourceOptions.PORT);
        options.add(SourceOptions.SCAN_STARTUP_MODE);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE);
        options.add(SourceOptions.CHUNK_META_GROUP_SIZE);
        options.add(SourceOptions.SCAN_SNAPSHOT_FETCH_SIZE);
        options.add(JdbcSourceOptions.CONNECT_TIMEOUT);
        options.add(JdbcSourceOptions.CONNECT_MAX_RETRIES);
        options.add(JdbcSourceOptions.CONNECTION_POOL_SIZE);
        options.add(SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND);
        options.add(SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND);
        options.add(JdbcSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_KEY_COLUMN);
        options.add(JdbcSourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        return options;
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";
    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SourceOptions.SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();

            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s], but was: %s",
                                SourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
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
                        SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND.key(),
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
                        SourceOptions.SPLIT_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND.key(),
                        0.0d,
                        1.0d,
                        distributionFactorLower));
    }
}
