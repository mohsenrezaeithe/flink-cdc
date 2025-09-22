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

package org.apache.flink.cdc.connectors.mongodb.source.reader;

import org.apache.flink.cdc.connectors.base.source.meta.split.SnapshotSplit;
import org.apache.flink.cdc.connectors.mongodb.source.MongoDBSourceTestBase;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SampleBucketSplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SingleSplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SplitContext;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.assigners.splitters.SplitVectorSplitStrategy;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfig;
import org.apache.flink.cdc.connectors.mongodb.source.config.MongoDBSourceConfigFactory;
import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer;

import io.debezium.relational.TableId;
import org.assertj.core.api.Assertions;
import org.bson.BsonDocument;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.LinkedList;
import java.util.stream.Stream;

import static org.apache.flink.cdc.connectors.mongodb.internal.MongoDBEnvelope.BSON_MAX_KEY;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER;
import static org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer.FLINK_USER_PASSWORD;

/** MongoDB snapshot split reader test case. */
class MongoDBSnapshotSplitReaderAssignEndingFirstTest extends MongoDBSourceTestBase {

    private static final Logger LOG =
            LoggerFactory.getLogger(MongoDBSnapshotSplitReaderAssignEndingFirstTest.class);

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

    private SplitContext splitContext;

    @BeforeEach
    void before() {
        String database = MONGO_CONTAINER.executeCommandFileInSeparateDatabase("chunk_test");

        MongoDBSourceConfigFactory configFactory =
                new MongoDBSourceConfigFactory()
                        .hosts(MONGO_CONTAINER.getHostAndPort())
                        .databaseList(database)
                        .collectionList(database + ".shopping_cart")
                        .username(FLINK_USER)
                        .password(FLINK_USER_PASSWORD)
                        .splitSizeMB(1)
                        .samplesPerChunk(10)
                        .pollAwaitTimeMillis(500)
                        .assignUnboundedChunkFirst(true);

        MongoDBSourceConfig sourceConfig = configFactory.create(0);

        splitContext = SplitContext.of(sourceConfig, new TableId(database, null, "shopping_cart"));
    }

    @Test
    void testMongoDBSnapshotSplitReaderWithSplitVectorSplitter() {
        testMongoDBSnapshotSplitReader(SplitVectorSplitStrategy.INSTANCE);
    }

    @Test
    void testMongoDBSnapshotSplitReaderWithSamplerSplitter() {
        testMongoDBSnapshotSplitReader(SampleBucketSplitStrategy.INSTANCE);
    }

    @Test
    void testMongoDBSnapshotSplitReaderWithSingleSplitter() {
        testMongoDBSnapshotSplitReader(SingleSplitStrategy.INSTANCE);
    }

    private void testMongoDBSnapshotSplitReader(SplitStrategy splitter) {
        LinkedList<SnapshotSplit> snapshotSplits = new LinkedList<>(splitter.split(splitContext));
        Assertions.assertThat(snapshotSplits)
                .isNotEmpty()
                .first()
                .extracting(SnapshotSplit::getSplitEnd)
                .extracting(o -> o[1])
                .isInstanceOf(BsonDocument.class)
                .extracting(o -> ((BsonDocument) o).get("_id"))
                .isEqualTo(BSON_MAX_KEY);
    }
}
