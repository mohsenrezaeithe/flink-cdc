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

import org.apache.flink.cdc.connectors.mongodb.utils.MongoDBContainer;
import org.apache.flink.runtime.testutils.InMemoryReporter;
import org.apache.flink.test.util.AbstractTestBase;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

/** MongoDBSourceTestBase for MongoDB >= 5.0.3. */
public class MongoDBSourceTestBase extends AbstractTestBase {

    private static final Logger LOG = LoggerFactory.getLogger(MongoDBSourceTestBase.class);

    protected InMemoryReporter metricReporter = InMemoryReporter.createWithRetainedMetrics();

    public static String getMongoVersion() {
        String specifiedMongoVersion = System.getProperty("specifiedMongoVersion");
        if (Objects.isNull(specifiedMongoVersion)) {
            specifiedMongoVersion = "7.0.12";
            LOG.info(
                    "No MongoDB version was specified to run this test. Using {}",
                    specifiedMongoVersion);
        }
        return specifiedMongoVersion;
    }

    protected static final int DEFAULT_PARALLELISM = 4;

    protected MongoClient createClients(MongoDBContainer container) {
        MongoClientSettings settings =
                MongoClientSettings.builder()
                        .applyConnectionString(
                                new ConnectionString(container.getConnectionString()))
                        .build();
        return MongoClients.create(settings);
    }
}
