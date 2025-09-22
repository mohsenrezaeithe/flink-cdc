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

package org.apache.flink.cdc.connectors.mongodb.table;

import org.apache.flink.cdc.connectors.base.options.SourceOptions;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.utils.OptionUtils;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSource;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceOptions;
import org.apache.flink.cdc.debezium.utils.ResolvedSchemaUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.util.Preconditions;

import java.time.ZoneId;
import java.util.HashSet;
import java.util.Set;

/** Factory for creating configured instance of {@link MongoDBSource}. */
public class MongoDBTableSourceFactory implements DynamicTableSourceFactory {

    private static final String IDENTIFIER = "mongodb-cdc";

    private static final String DOCUMENT_ID_FIELD = "_id";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        helper.validate();

        final ReadableConfig config = helper.getOptions();

        String scheme = config.get(MongoDBSourceOptions.SCHEME);
        String hosts = config.get(MongoDBSourceOptions.HOSTS);
        String connectionOptions =
                config.getOptional(MongoDBSourceOptions.CONNECTION_OPTIONS).orElse(null);

        String username = config.getOptional(MongoDBSourceOptions.USERNAME).orElse(null);
        String password = config.getOptional(MongoDBSourceOptions.PASSWORD).orElse(null);

        String database = config.getOptional(MongoDBSourceOptions.DATABASE).orElse(null);
        String collection = config.getOptional(MongoDBSourceOptions.COLLECTION).orElse(null);

        Integer batchSize = config.get(MongoDBSourceOptions.BATCH_SIZE);
        Integer pollMaxBatchSize = config.get(MongoDBSourceOptions.POLL_MAX_BATCH_SIZE);
        Integer pollAwaitTimeMillis = config.get(MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS);

        Integer heartbeatIntervalMillis =
                config.get(MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS);

        StartupOptions startupOptions = getStartupOptions(config);
        Integer initialSnapshottingQueueSize =
                config.getOptional(MongoDBSourceOptions.INITIAL_SNAPSHOTTING_QUEUE_SIZE)
                        .orElse(null);
        Integer initialSnapshottingMaxThreads =
                config.getOptional(MongoDBSourceOptions.INITIAL_SNAPSHOTTING_MAX_THREADS)
                        .orElse(null);
        String initialSnapshottingPipeline =
                config.getOptional(MongoDBSourceOptions.INITIAL_SNAPSHOTTING_PIPELINE).orElse(null);

        String zoneId = context.getConfiguration().get(TableConfigOptions.LOCAL_TIME_ZONE);
        ZoneId localTimeZone =
                TableConfigOptions.LOCAL_TIME_ZONE.defaultValue().equals(zoneId)
                        ? ZoneId.systemDefault()
                        : ZoneId.of(zoneId);

        boolean enableParallelRead =
                config.get(MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);

        // The initial.snapshotting.pipeline related config is only used in Debezium mode and
        // cannot be used in incremental snapshot mode because the semantic is inconsistent.
        // The reason is that in snapshot phase of incremental snapshot mode, the oplog
        // will be backfilled after each snapshot to compensate for changes, but the pipeline
        // operations in initial.snapshotting.pipeline are not applied to the backfill oplog,
        // which means the semantic of this config is inconsistent.
        Preconditions.checkArgument(
                !(enableParallelRead
                        && (initialSnapshottingPipeline != null
                                || initialSnapshottingMaxThreads != null
                                || initialSnapshottingQueueSize != null)),
                "The initial.snapshotting.*/copy.existing.* config only applies to Debezium mode, "
                        + "not incremental snapshot mode");

        boolean enableCloseIdleReaders =
                config.get(SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        boolean skipSnapshotBackfill =
                config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        boolean scanNewlyAddedTableEnabled =
                config.get(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        boolean assignUnboundedChunkFirst =
                config.get(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);

        int splitSizeMB = config.get(MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        int splitMetaGroupSize = config.get(SourceOptions.CHUNK_META_GROUP_SIZE);
        int samplesPerChunk =
                config.get(MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);

        boolean enableFullDocumentPrePostImage =
                config.getOptional(MongoDBSourceOptions.FULL_DOCUMENT_PRE_POST_IMAGE).orElse(false);

        boolean noCursorTimeout =
                config.getOptional(MongoDBSourceOptions.SCAN_NO_CURSOR_TIMEOUT).orElse(true);
        ResolvedSchema physicalSchema =
                ResolvedSchemaUtils.getPhysicalSchema(
                        context.getCatalogTable().getResolvedSchema());
        Preconditions.checkArgument(
                physicalSchema.getPrimaryKey().isPresent(), "Primary key must be present");
        checkPrimaryKey(physicalSchema.getPrimaryKey().get(), "Primary key must be _id field");

        OptionUtils.printOptions(IDENTIFIER, config.toMap());

        return new MongoDBTableSource(
                physicalSchema,
                scheme,
                hosts,
                username,
                password,
                database,
                collection,
                connectionOptions,
                startupOptions,
                initialSnapshottingQueueSize,
                initialSnapshottingMaxThreads,
                initialSnapshottingPipeline,
                batchSize,
                pollMaxBatchSize,
                pollAwaitTimeMillis,
                heartbeatIntervalMillis,
                localTimeZone,
                splitMetaGroupSize,
                splitSizeMB,
                samplesPerChunk,
                enableCloseIdleReaders,
                enableFullDocumentPrePostImage,
                noCursorTimeout,
                skipSnapshotBackfill,
                scanNewlyAddedTableEnabled,
                assignUnboundedChunkFirst);
    }

    private void checkPrimaryKey(UniqueConstraint pk, String message) {
        Preconditions.checkArgument(
                pk.getColumns().size() == 1 && pk.getColumns().contains(DOCUMENT_ID_FIELD),
                message);
    }

    private static final String SCAN_STARTUP_MODE_VALUE_INITIAL = "initial";

    private static final String SCAN_STARTUP_MODE_VALUE_SNAPSHOT = "snapshot";
    private static final String SCAN_STARTUP_MODE_VALUE_LATEST = "latest-offset";
    private static final String SCAN_STARTUP_MODE_VALUE_TIMESTAMP = "timestamp";

    private static StartupOptions getStartupOptions(ReadableConfig config) {
        String modeString = config.get(SourceOptions.SCAN_STARTUP_MODE);

        switch (modeString.toLowerCase()) {
            case SCAN_STARTUP_MODE_VALUE_INITIAL:
                return StartupOptions.initial();
            case SCAN_STARTUP_MODE_VALUE_SNAPSHOT:
                return StartupOptions.snapshot();
            case SCAN_STARTUP_MODE_VALUE_LATEST:
                return StartupOptions.latest();
            case SCAN_STARTUP_MODE_VALUE_TIMESTAMP:
                return StartupOptions.timestamp(
                        Preconditions.checkNotNull(
                                config.get(SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS),
                                String.format(
                                        "To use timestamp startup mode, the startup timestamp millis '%s' must be set.",
                                        SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS.key())));
            default:
                throw new ValidationException(
                        String.format(
                                "Invalid value for option '%s'. Supported values are [%s, %s, %s, %s], but was: %s",
                                SourceOptions.SCAN_STARTUP_MODE.key(),
                                SCAN_STARTUP_MODE_VALUE_INITIAL,
                                SCAN_STARTUP_MODE_VALUE_SNAPSHOT,
                                SCAN_STARTUP_MODE_VALUE_LATEST,
                                SCAN_STARTUP_MODE_VALUE_TIMESTAMP,
                                modeString));
        }
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MongoDBSourceOptions.HOSTS);
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(MongoDBSourceOptions.SCHEME);
        options.add(MongoDBSourceOptions.USERNAME);
        options.add(MongoDBSourceOptions.PASSWORD);
        options.add(MongoDBSourceOptions.CONNECTION_OPTIONS);
        options.add(MongoDBSourceOptions.DATABASE);
        options.add(MongoDBSourceOptions.COLLECTION);
        options.add(SourceOptions.SCAN_STARTUP_MODE);
        options.add(SourceOptions.SCAN_STARTUP_TIMESTAMP_MILLIS);
        options.add(MongoDBSourceOptions.INITIAL_SNAPSHOTTING_QUEUE_SIZE);
        options.add(MongoDBSourceOptions.INITIAL_SNAPSHOTTING_MAX_THREADS);
        options.add(MongoDBSourceOptions.INITIAL_SNAPSHOTTING_PIPELINE);
        options.add(MongoDBSourceOptions.BATCH_SIZE);
        options.add(MongoDBSourceOptions.POLL_MAX_BATCH_SIZE);
        options.add(MongoDBSourceOptions.POLL_AWAIT_TIME_MILLIS);
        options.add(MongoDBSourceOptions.HEARTBEAT_INTERVAL_MILLIS);
        options.add(MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_ENABLED);
        options.add(MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB);
        options.add(MongoDBSourceOptions.SCAN_INCREMENTAL_SNAPSHOT_CHUNK_SAMPLES);
        options.add(SourceOptions.CHUNK_META_GROUP_SIZE);
        options.add(SourceOptions.SCAN_INCREMENTAL_CLOSE_IDLE_READER_ENABLED);
        options.add(MongoDBSourceOptions.FULL_DOCUMENT_PRE_POST_IMAGE);
        options.add(MongoDBSourceOptions.SCAN_NO_CURSOR_TIMEOUT);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_BACKFILL_SKIP);
        options.add(SourceOptions.SCAN_NEWLY_ADDED_TABLE_ENABLED);
        options.add(SourceOptions.SCAN_INCREMENTAL_SNAPSHOT_UNBOUNDED_CHUNK_FIRST_ENABLED);
        return options;
    }
}
