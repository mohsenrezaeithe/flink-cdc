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

package org.apache.flink.cdc.connectors.mongodb.source;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cdc.connectors.base.options.StartupOptions;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHook;
import org.apache.flink.cdc.connectors.base.source.utils.hooks.SnapshotPhaseHooks;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.source.utils.MongoUtils;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBAssertUtils;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBTestUtils;
import org.apache.flink.cdc.connectors.mongodb.utils.TestTable;
import org.apache.flink.cdc.connectors.utils.ExternalResourceProxy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.catalog.UniqueConstraint;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Preconditions;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Updates;
import org.assertj.core.api.Assertions;
import org.bson.Document;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Integration tests for MongoDB full document before change info. */
@Timeout(value = 300, unit = TimeUnit.SECONDS)
class MongoDBFullChangelogITCase extends MongoDBSourceTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBFullChangelogITCase.class);

    protected MongoClient mongodbClient;

    private static final MongoDBContainer MONGO_CONTAINER =
            new MongoDBContainer("mongo:" + getMongoVersion())
                    .withSharding()
                    .withLogConsumer(new Slf4jLogConsumer(LOG));

    @BeforeAll
    static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MONGO_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @AfterAll
    static void stopContainers() {
        LOG.info("Stopping containers...");
        if (MONGO_CONTAINER != null) {
            MONGO_CONTAINER.stop();
        }
        LOG.info("Containers are stopped.");
    }

    @BeforeEach
    public void testSetup() {
        mongodbClient = this.createClients(MONGO_CONTAINER);
    }

    @AfterEach
    public void testDestroy() {
        if (mongodbClient != null) {
            mongodbClient.close();
            mongodbClient = null;
        }
    }

    @RegisterExtension
    public final ExternalResourceProxy<MiniClusterWithClientResource> miniClusterResource =
            new ExternalResourceProxy<>(
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setNumberTaskManagers(1)
                                    .setNumberSlotsPerTaskManager(DEFAULT_PARALLELISM)
                                    .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                    .setConfiguration(
                                            metricReporter.addToConfiguration(new Configuration()))
                                    .withHaLeadershipControl()
                                    .build()));

    private static final int USE_POST_LOWWATERMARK_HOOK = 1;
    private static final int USE_PRE_HIGHWATERMARK_HOOK = 2;
    private static final int USE_POST_HIGHWATERMARK_HOOK = 3;

    private static final StreamExecutionEnvironment env;

    static {
        final Configuration conf = new Configuration();
        conf.set(
                RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.FIXED_DELAY.getMainValue());
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS, 1);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(0));
        env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
    }

    @Test
    void testGetMongoDBVersion() {
        MongoDBSourceConfig config =
                new MongoDBSourceConfigFactory()
                        .hosts(MONGO_CONTAINER.getHostAndPort())
                        .splitSizeMB(1)
                        .samplesPerChunk(10)
                        .pollAwaitTimeMillis(500)
                        .create(0);

        Assertions.assertThat(MongoUtils.getMongoVersion(config)).isEqualTo(getMongoVersion());
    }

    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testReadSingleCollectionWithSingleParallelism(boolean parallelismSnapshot)
            throws Exception {
        testMongoDBParallelSource(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                new String[] {"customers"},
                parallelismSnapshot);
    }

    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testReadSingleCollectionWithMultipleParallelism(boolean parallelismSnapshot)
            throws Exception {
        testMongoDBParallelSource(
                4,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                new String[] {"customers"},
                parallelismSnapshot);
    }

    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testReadMultipleCollectionWithSingleParallelism(boolean parallelismSnapshot)
            throws Exception {
        testMongoDBParallelSource(
                1,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"},
                parallelismSnapshot);
    }

    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    void testReadMultipleCollectionWithMultipleParallelism(boolean parallelismSnapshot)
            throws Exception {
        testMongoDBParallelSource(
                4,
                MongoDBTestUtils.FailoverType.NONE,
                MongoDBTestUtils.FailoverPhase.NEVER,
                new String[] {"customers", "customers_1"},
                parallelismSnapshot);
    }

    // Failover tests
    @Test
    void testTaskManagerFailoverInSnapshotPhase() throws Exception {
        testMongoDBParallelSource(
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"customers", "customers_1"},
                true);
    }

    @Test
    void testTaskManagerFailoverInStreamPhase() throws Exception {
        testMongoDBParallelSource(
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.STREAM,
                new String[] {"customers", "customers_1"},
                true);
    }

    @Test
    void testJobManagerFailoverInSnapshotPhase() throws Exception {
        testMongoDBParallelSource(
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"customers", "customers_1"},
                true);
    }

    @Test
    void testJobManagerFailoverInStreamPhase() throws Exception {
        testMongoDBParallelSource(
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.STREAM,
                new String[] {"customers", "customers_1"},
                true);
    }

    @Test
    void testTaskManagerFailoverSingleParallelism() throws Exception {
        testMongoDBParallelSource(
                1,
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"customers"},
                true);
    }

    @Test
    void testJobManagerFailoverSingleParallelism() throws Exception {
        testMongoDBParallelSource(
                1,
                MongoDBTestUtils.FailoverType.JM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"customers"},
                true);
    }

    @Test
    void testReadSingleTableWithSingleParallelismAndSkipBackfill() throws Exception {
        testMongoDBParallelSource(
                DEFAULT_PARALLELISM,
                MongoDBTestUtils.FailoverType.TM,
                MongoDBTestUtils.FailoverPhase.SNAPSHOT,
                new String[] {"customers"},
                true);
    }

    @Test
    void testSnapshotOnlyModeWithDMLPostHighWaterMark() throws Exception {
        // The data num is 21, set fetchSize = 22 to test whether the job is bounded.
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 22, USE_POST_HIGHWATERMARK_HOOK, StartupOptions.snapshot());
        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[1019, user_20, Shanghai, 123567891234]",
                        "+I[2000, user_21, Shanghai, 123567891234]");
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testSnapshotOnlyModeWithDMLPreHighWaterMark() throws Exception {
        // The data num is 21, set fetchSize = 22 to test whether the job is bounded
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 22, USE_PRE_HIGHWATERMARK_HOOK, StartupOptions.snapshot());
        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[2000, user_21, Pittsburgh, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]");
        // when enable backfill, the wal log between (snapshot, high_watermark) will be
        // applied as snapshot image
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testEnableBackfillWithDMLPreHighWaterMark() throws Exception {
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 21, USE_PRE_HIGHWATERMARK_HOOK, StartupOptions.initial());

        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[2000, user_21, Pittsburgh, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]");
        // when enable backfill, the wal log between (snapshot, high_watermark) will be
        // applied as snapshot image
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testEnableBackfillWithDMLPostLowWaterMark() throws Exception {
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 21, USE_POST_LOWWATERMARK_HOOK, StartupOptions.initial());

        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[2000, user_21, Pittsburgh, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]");
        // when enable backfill, the wal log between (low_watermark, snapshot) will be applied
        // as snapshot image
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testEnableBackfillWithDMLPostHighWaterMark() throws Exception {
        List<String> records =
                testBackfillWhenWritingEvents(
                        false, 25, USE_POST_HIGHWATERMARK_HOOK, StartupOptions.initial());
        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[1019, user_20, Shanghai, 123567891234]",
                        "+I[2000, user_21, Shanghai, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "-U[2000, user_21, Shanghai, 123567891234]",
                        "+U[2000, user_21, Pittsburgh, 123567891234]",
                        "-D[1019, user_20, Shanghai, 123567891234]");
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testSkipBackfillWithDMLPreHighWaterMark() throws Exception {
        List<String> records =
                testBackfillWhenWritingEvents(
                        true, 25, USE_PRE_HIGHWATERMARK_HOOK, StartupOptions.initial());

        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[1019, user_20, Shanghai, 123567891234]",
                        "+I[2000, user_21, Shanghai, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "-U[2000, user_21, Shanghai, 123567891234]",
                        "+U[2000, user_21, Pittsburgh, 123567891234]",
                        "-D[1019, user_20, Shanghai, 123567891234]");
        // when skip backfill, the wal log between (snapshot, high_watermark) will be seen as
        // stream event.
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedRecords, records);
    }

    @Test
    void testSkipBackfillWithDMLPostLowWaterMark() throws Exception {
        List<String> records =
                testBackfillWhenWritingEvents(
                        true, 25, USE_POST_LOWWATERMARK_HOOK, StartupOptions.initial());

        List<String> expectedRecords =
                Arrays.asList(
                        "+I[101, user_1, Shanghai, 123567891234]",
                        "+I[102, user_2, Shanghai, 123567891234]",
                        "+I[103, user_3, Shanghai, 123567891234]",
                        "+I[109, user_4, Shanghai, 123567891234]",
                        "+I[110, user_5, Shanghai, 123567891234]",
                        "+I[111, user_6, Shanghai, 123567891234]",
                        "+I[118, user_7, Shanghai, 123567891234]",
                        "+I[121, user_8, Shanghai, 123567891234]",
                        "+I[123, user_9, Shanghai, 123567891234]",
                        "+I[1009, user_10, Shanghai, 123567891234]",
                        "+I[1010, user_11, Shanghai, 123567891234]",
                        "+I[1011, user_12, Shanghai, 123567891234]",
                        "+I[1012, user_13, Shanghai, 123567891234]",
                        "+I[1013, user_14, Shanghai, 123567891234]",
                        "+I[1014, user_15, Shanghai, 123567891234]",
                        "+I[1015, user_16, Shanghai, 123567891234]",
                        "+I[1016, user_17, Shanghai, 123567891234]",
                        "+I[1017, user_18, Shanghai, 123567891234]",
                        "+I[1018, user_19, Shanghai, 123567891234]",
                        "+I[2000, user_21, Pittsburgh, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "+I[15213, user_15213, Shanghai, 123567891234]",
                        "-U[2000, user_21, Shanghai, 123567891234]",
                        "+U[2000, user_21, Pittsburgh, 123567891234]",
                        "-D[1019, user_20, Shanghai, 123567891234]");
        // when skip backfill, the wal log between (snapshot,  high_watermark) will still be
        // seen as stream event. This will occur data duplicate. For example, user_20 will be
        // deleted twice, and user_15213 will be inserted twice.
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedRecords, records);
    }

    private List<String> testBackfillWhenWritingEvents(
            boolean skipBackFill, int fetchSize, int hookType, StartupOptions startupOptions)
            throws Exception {

        String customerDatabase =
                "customer_" + Integer.toUnsignedString(new Random().nextInt(), 36);

        // A - enable system-level fulldoc pre & post image feature
        MONGO_CONTAINER.executeCommand(
                "use admin; db.runCommand({ setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } })");

        // B - enable collection-level fulldoc pre & post image for change capture collection
        MONGO_CONTAINER.executeCommandInDatabase(
                String.format(
                        "db.createCollection('%s'); db.runCommand({ collMod: '%s', changeStreamPreAndPostImages: { enabled: true } })",
                        "customers", "customers"),
                customerDatabase);
        MONGO_CONTAINER.executeCommandFileInDatabase("customer", customerDatabase);

        env.enableCheckpointing(1000);
        env.setParallelism(1);

        ResolvedSchema customersSchema =
                new ResolvedSchema(
                        Arrays.asList(
                                Column.physical("cid", DataTypes.BIGINT().notNull()),
                                Column.physical("name", DataTypes.STRING()),
                                Column.physical("address", DataTypes.STRING()),
                                Column.physical("phone_number", DataTypes.STRING())),
                        new ArrayList<>(),
                        UniqueConstraint.primaryKey("pk", Collections.singletonList("cid")));
        TestTable customerTable = new TestTable(customerDatabase, "customers", customersSchema);
        MongoDBSource<RowData> source =
                new MongoDBSourceBuilder<RowData>()
                        .hosts(MONGO_CONTAINER.getHostAndPort())
                        .databaseList(customerDatabase)
                        .username(MongoDBContainer.FLINK_USER)
                        .password(MongoDBContainer.FLINK_USER_PASSWORD)
                        .startupOptions(startupOptions)
                        .scanFullChangelog(true)
                        .collectionList(
                                getCollectionNameRegex(
                                        customerDatabase, new String[] {"customers"}))
                        .deserializer(customerTable.getDeserializer(true))
                        .skipSnapshotBackfill(skipBackFill)
                        .build();

        // Do some database operations during hook in snapshot phase.
        SnapshotPhaseHooks hooks = new SnapshotPhaseHooks();
        SnapshotPhaseHook snapshotPhaseHook =
                (sourceConfig, split) -> {
                    MongoDBSourceConfig mongoDBSourceConfig = (MongoDBSourceConfig) sourceConfig;
                    MongoClient mongoClient = MongoUtils.clientFor(mongoDBSourceConfig);
                    MongoDatabase database =
                            mongoClient.getDatabase(mongoDBSourceConfig.getDatabaseList().get(0));
                    MongoCollection<Document> mongoCollection = database.getCollection("customers");
                    Document document = new Document();
                    document.put("cid", 15213L);
                    document.put("name", "user_15213");
                    document.put("address", "Shanghai");
                    document.put("phone_number", "123567891234");
                    mongoCollection.insertOne(document);
                    mongoCollection.updateOne(
                            Filters.eq("cid", 2000L), Updates.set("address", "Pittsburgh"));
                    mongoCollection.deleteOne(Filters.eq("cid", 1019L));

                    // Rarely happens, but if there's no operation or heartbeat events between
                    // watermark #a (the ChangeStream opLog caused by the last event in this hook)
                    // and watermark #b (the calculated high watermark that limits the bounded
                    // back-filling stream fetch task), the last event of hook will be missed since
                    // back-filling task reads between [loW, hiW) (high watermark not included).
                    // Workaround: insert a dummy event in another collection to forcefully push
                    // opLog forward.
                    database.getCollection("customers_1").insertOne(new Document());
                };

        switch (hookType) {
            case USE_POST_LOWWATERMARK_HOOK:
                hooks.setPostLowWatermarkAction(snapshotPhaseHook);
                break;
            case USE_PRE_HIGHWATERMARK_HOOK:
                hooks.setPreHighWatermarkAction(snapshotPhaseHook);
                break;
            case USE_POST_HIGHWATERMARK_HOOK:
                hooks.setPostHighWatermarkAction(snapshotPhaseHook);
                break;
        }
        source.setSnapshotHooks(hooks);

        try (CloseableIterator<RowData> iterator =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "Backfill Skipped Source")
                        .executeAndCollect()) {
            List<String> records =
                    MongoDBTestUtils.fetchRowData(iterator, fetchSize, customerTable::stringify);
            env.close();
            return records;
        }
    }

    @ParameterizedTest(name = "parallelismSnapshot: {0}")
    @ValueSource(booleans = {true, false})
    public void testMetadataColumns(boolean parallelismSnapshot) throws Exception {
        testMongoDBParallelSourceWithMetadataColumns(
                DEFAULT_PARALLELISM, new String[] {"customers"}, true, parallelismSnapshot);
    }

    private void testMongoDBParallelSourceWithMetadataColumns(
            int parallelism,
            String[] captureCustomerCollections,
            boolean skipSnapshotBackfill,
            boolean parallelismSnapshot)
            throws Exception {
        String customerDatabase =
                "customer_" + Integer.toUnsignedString(new Random().nextInt(), 36);

        // A - enable system-level fulldoc pre & post image feature
        MONGO_CONTAINER.executeCommand(
                "use admin; db.runCommand({ setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } })");

        // B - enable collection-level fulldoc pre & post image for change capture collection
        for (String collectionName : captureCustomerCollections) {
            MONGO_CONTAINER.executeCommandInDatabase(
                    String.format(
                            "db.createCollection('%s'); db.runCommand({ collMod: '%s', changeStreamPreAndPostImages: { enabled: true } })",
                            collectionName, collectionName),
                    customerDatabase);
        }

        final Configuration conf = new Configuration();
        conf.set(
                RestartStrategyOptions.RESTART_STRATEGY,
                RestartStrategyOptions.RestartStrategyType.FIXED_DELAY.getMainValue());
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS, 1);
        conf.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofSeconds(0));
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);

        String sourceDDL =
                String.format(
                        "CREATE TABLE customers ("
                                + " _id STRING NOT NULL,"
                                + " cid BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " database_name STRING METADATA VIRTUAL,"
                                + " collection_name STRING METADATA VIRTUAL,"
                                + " row_kind STRING METADATA VIRTUAL,"
                                + " primary key (_id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mongodb-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'hosts' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database' = '%s',"
                                + " 'collection' = '%s',"
                                + " 'heartbeat.interval.ms' = '500',"
                                + " 'scan.full-changelog' = 'true',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s'"
                                + ")",
                        parallelismSnapshot ? "true" : "false",
                        MONGO_CONTAINER.getHostAndPort(),
                        MongoDBContainer.FLINK_USER,
                        MongoDBContainer.FLINK_USER_PASSWORD,
                        customerDatabase,
                        getCollectionNameRegex(customerDatabase, captureCustomerCollections),
                        skipSnapshotBackfill);

        MONGO_CONTAINER.executeCommandFileInDatabase("customer", customerDatabase);

        // first step: check the snapshot data
        List<String> snapshotForSingleTable =
                Stream.of(
                                "+I[%s, %s, +I, 101, user_1, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 102, user_2, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 103, user_3, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 109, user_4, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 110, user_5, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 111, user_6, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 118, user_7, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 121, user_8, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 123, user_9, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1009, user_10, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1010, user_11, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1011, user_12, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1012, user_13, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1013, user_14, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1014, user_15, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1015, user_16, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1016, user_17, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1017, user_18, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1018, user_19, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 1019, user_20, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 2000, user_21, Shanghai, 123567891234]")
                        .map(s -> String.format(s, customerDatabase, captureCustomerCollections[0]))
                        .collect(Collectors.toList());

        tEnv.executeSql(sourceDDL);
        TableResult tableResult =
                tEnv.executeSql(
                        "select database_name, collection_name, row_kind, "
                                + "cid, name, address, phone_number from customers");
        CloseableIterator<Row> iterator = tableResult.collect();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerCollections.length; i++) {
            expectedSnapshotData.addAll(snapshotForSingleTable);
        }

        MongoDBAssertUtils.assertEqualsInAnyOrder(
                expectedSnapshotData,
                MongoDBTestUtils.fetchRows(iterator, expectedSnapshotData.size()));

        // second step: check the change stream data
        for (String collectionName : captureCustomerCollections) {
            makeFirstPartChangeStreamEvents(
                    mongodbClient.getDatabase(customerDatabase), collectionName);
        }
        for (String collectionName : captureCustomerCollections) {
            makeSecondPartChangeStreamEvents(
                    mongodbClient.getDatabase(customerDatabase), collectionName);
        }

        List<String> changeEventsForSingleTable =
                Stream.of(
                                "-U[%s, %s, -U, 101, user_1, Shanghai, 123567891234]",
                                "+U[%s, %s, +U, 101, user_1, Hangzhou, 123567891234]",
                                "-D[%s, %s, -D, 102, user_2, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 102, user_2, Shanghai, 123567891234]",
                                "-U[%s, %s, -U, 103, user_3, Shanghai, 123567891234]",
                                "+U[%s, %s, +U, 103, user_3, Hangzhou, 123567891234]",
                                "-U[%s, %s, -U, 1010, user_11, Shanghai, 123567891234]",
                                "+U[%s, %s, +U, 1010, user_11, Hangzhou, 123567891234]",
                                "+I[%s, %s, +I, 2001, user_22, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 2002, user_23, Shanghai, 123567891234]",
                                "+I[%s, %s, +I, 2003, user_24, Shanghai, 123567891234]")
                        .map(s -> String.format(s, customerDatabase, captureCustomerCollections[0]))
                        .collect(Collectors.toList());
        List<String> expectedChangeStreamData = new ArrayList<>();
        for (int i = 0; i < captureCustomerCollections.length; i++) {
            expectedChangeStreamData.addAll(changeEventsForSingleTable);
        }
        List<String> actualChangeStreamData =
                MongoDBTestUtils.fetchRows(iterator, expectedChangeStreamData.size());
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedChangeStreamData, actualChangeStreamData);
        tableResult.getJobClient().orElseThrow().cancel().get();
    }

    private void testMongoDBParallelSource(
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerCollections,
            boolean parallelismSnapshot)
            throws Exception {
        testMongoDBParallelSource(
                DEFAULT_PARALLELISM,
                failoverType,
                failoverPhase,
                captureCustomerCollections,
                parallelismSnapshot);
    }

    private void testMongoDBParallelSource(
            int parallelism,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerCollections,
            boolean parallelismSnapshot)
            throws Exception {
        testMongoDBParallelSource(
                parallelism,
                failoverType,
                failoverPhase,
                captureCustomerCollections,
                false,
                parallelismSnapshot);
    }

    private void testMongoDBParallelSource(
            int parallelism,
            MongoDBTestUtils.FailoverType failoverType,
            MongoDBTestUtils.FailoverPhase failoverPhase,
            String[] captureCustomerCollections,
            boolean skipSnapshotBackfill,
            boolean parallelismSnapshot)
            throws Exception {

        String customerDatabase =
                "customer_" + Integer.toUnsignedString(new Random().nextInt(), 36);

        // A - enable system-level fulldoc pre & post image feature
        MONGO_CONTAINER.executeCommand(
                "use admin; db.runCommand({ setClusterParameter: { changeStreamOptions: { preAndPostImages: { expireAfterSeconds: 'off' } } } })");

        // B - enable collection-level fulldoc pre & post image for change capture collection
        for (String collectionName : captureCustomerCollections) {
            MONGO_CONTAINER.executeCommandInDatabase(
                    String.format(
                            "db.createCollection('%s'); db.runCommand({ collMod: '%s', changeStreamPreAndPostImages: { enabled: true } })",
                            collectionName, collectionName),
                    customerDatabase);
        }

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        env.setParallelism(parallelism);
        env.enableCheckpointing(200L);

        String sourceDDL =
                String.format(
                        "CREATE TABLE customers ("
                                + " _id STRING NOT NULL,"
                                + " cid BIGINT NOT NULL,"
                                + " name STRING,"
                                + " address STRING,"
                                + " phone_number STRING,"
                                + " primary key (_id) not enforced"
                                + ") WITH ("
                                + " 'connector' = 'mongodb-cdc',"
                                + " 'scan.incremental.snapshot.enabled' = '%s',"
                                + " 'hosts' = '%s',"
                                + " 'username' = '%s',"
                                + " 'password' = '%s',"
                                + " 'database' = '%s',"
                                + " 'collection' = '%s',"
                                + " 'heartbeat.interval.ms' = '500',"
                                + " 'scan.full-changelog' = 'true',"
                                + " 'scan.incremental.snapshot.backfill.skip' = '%s'"
                                + ")",
                        parallelismSnapshot ? "true" : "false",
                        MONGO_CONTAINER.getHostAndPort(),
                        MongoDBContainer.FLINK_USER,
                        MongoDBContainer.FLINK_USER_PASSWORD,
                        customerDatabase,
                        getCollectionNameRegex(customerDatabase, captureCustomerCollections),
                        skipSnapshotBackfill);

        MONGO_CONTAINER.executeCommandFileInDatabase("customer", customerDatabase);

        // first step: check the snapshot data
        String[] snapshotForSingleTable =
                new String[] {
                    "+I[101, user_1, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "+I[103, user_3, Shanghai, 123567891234]",
                    "+I[109, user_4, Shanghai, 123567891234]",
                    "+I[110, user_5, Shanghai, 123567891234]",
                    "+I[111, user_6, Shanghai, 123567891234]",
                    "+I[118, user_7, Shanghai, 123567891234]",
                    "+I[121, user_8, Shanghai, 123567891234]",
                    "+I[123, user_9, Shanghai, 123567891234]",
                    "+I[1009, user_10, Shanghai, 123567891234]",
                    "+I[1010, user_11, Shanghai, 123567891234]",
                    "+I[1011, user_12, Shanghai, 123567891234]",
                    "+I[1012, user_13, Shanghai, 123567891234]",
                    "+I[1013, user_14, Shanghai, 123567891234]",
                    "+I[1014, user_15, Shanghai, 123567891234]",
                    "+I[1015, user_16, Shanghai, 123567891234]",
                    "+I[1016, user_17, Shanghai, 123567891234]",
                    "+I[1017, user_18, Shanghai, 123567891234]",
                    "+I[1018, user_19, Shanghai, 123567891234]",
                    "+I[1019, user_20, Shanghai, 123567891234]",
                    "+I[2000, user_21, Shanghai, 123567891234]"
                };
        tEnv.executeSql(sourceDDL);
        TableResult tableResult =
                tEnv.executeSql("select cid, name, address, phone_number from customers");
        CloseableIterator<Row> iterator = tableResult.collect();
        JobID jobId = tableResult.getJobClient().orElseThrow().getJobID();
        List<String> expectedSnapshotData = new ArrayList<>();
        for (int i = 0; i < captureCustomerCollections.length; i++) {
            expectedSnapshotData.addAll(Arrays.asList(snapshotForSingleTable));
        }

        // trigger failover after some snapshot splits read finished
        if (failoverPhase == MongoDBTestUtils.FailoverPhase.SNAPSHOT && iterator.hasNext()) {
            MongoDBTestUtils.triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(100));
        }

        MongoDBAssertUtils.assertEqualsInAnyOrder(
                expectedSnapshotData,
                MongoDBTestUtils.fetchRows(iterator, expectedSnapshotData.size()));

        // second step: check the change stream data
        for (String collectionName : captureCustomerCollections) {
            makeFirstPartChangeStreamEvents(
                    mongodbClient.getDatabase(customerDatabase), collectionName);
        }
        if (failoverPhase == MongoDBTestUtils.FailoverPhase.STREAM) {
            MongoDBTestUtils.triggerFailover(
                    failoverType,
                    jobId,
                    miniClusterResource.get().getMiniCluster(),
                    () -> sleepMs(200));
        }
        for (String collectionName : captureCustomerCollections) {
            makeSecondPartChangeStreamEvents(
                    mongodbClient.getDatabase(customerDatabase), collectionName);
        }

        String[] changeEventsForSingleTable =
                new String[] {
                    "-U[101, user_1, Shanghai, 123567891234]",
                    "+U[101, user_1, Hangzhou, 123567891234]",
                    "-D[102, user_2, Shanghai, 123567891234]",
                    "+I[102, user_2, Shanghai, 123567891234]",
                    "-U[103, user_3, Shanghai, 123567891234]",
                    "+U[103, user_3, Hangzhou, 123567891234]",
                    "-U[1010, user_11, Shanghai, 123567891234]",
                    "+U[1010, user_11, Hangzhou, 123567891234]",
                    "+I[2001, user_22, Shanghai, 123567891234]",
                    "+I[2002, user_23, Shanghai, 123567891234]",
                    "+I[2003, user_24, Shanghai, 123567891234]"
                };
        List<String> expectedChangeStreamData = new ArrayList<>();
        for (int i = 0; i < captureCustomerCollections.length; i++) {
            expectedChangeStreamData.addAll(Arrays.asList(changeEventsForSingleTable));
        }
        List<String> actualChangeStreamData =
                MongoDBTestUtils.fetchRows(iterator, expectedChangeStreamData.size());
        MongoDBAssertUtils.assertEqualsInAnyOrder(expectedChangeStreamData, actualChangeStreamData);
        tableResult.getJobClient().get().cancel().get();
    }

    private String getCollectionNameRegex(String database, String[] captureCustomerCollections) {
        Preconditions.checkState(captureCustomerCollections.length > 0);
        if (captureCustomerCollections.length == 1) {
            return database + "." + captureCustomerCollections[0];
        } else {
            // pattern that matches multiple collections
            return Arrays.stream(captureCustomerCollections)
                    .map(coll -> "^(" + database + "." + coll + ")$")
                    .collect(Collectors.joining("|"));
        }
    }

    private void sleepMs(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    private void makeFirstPartChangeStreamEvents(MongoDatabase mongoDatabase, String collection) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        mongoCollection.updateOne(Filters.eq("cid", 101L), Updates.set("address", "Hangzhou"));
        mongoCollection.deleteOne(Filters.eq("cid", 102L));
        mongoCollection.insertOne(customerDocOf(102L, "user_2", "Shanghai", "123567891234"));
        mongoCollection.updateOne(Filters.eq("cid", 103L), Updates.set("address", "Hangzhou"));
    }

    private void makeSecondPartChangeStreamEvents(MongoDatabase mongoDatabase, String collection) {
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection(collection);
        mongoCollection.updateOne(Filters.eq("cid", 1010L), Updates.set("address", "Hangzhou"));
        mongoCollection.insertMany(
                Arrays.asList(
                        customerDocOf(2001L, "user_22", "Shanghai", "123567891234"),
                        customerDocOf(2002L, "user_23", "Shanghai", "123567891234"),
                        customerDocOf(2003L, "user_24", "Shanghai", "123567891234")));
    }

    private Document customerDocOf(Long cid, String name, String address, String phoneNumber) {
        Document document = new Document();
        document.put("cid", cid);
        document.put("name", name);
        document.put("address", address);
        document.put("phone_number", phoneNumber);
        return document;
    }
}
