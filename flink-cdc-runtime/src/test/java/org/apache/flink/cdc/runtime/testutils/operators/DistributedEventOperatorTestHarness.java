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

package org.apache.flink.cdc.runtime.testutils.operators;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.distributed.SchemaCoordinator;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.testutils.schema.CollectingMetadataApplier;
import org.apache.flink.cdc.runtime.testutils.schema.TestingSchemaRegistryGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BaseEventOperatorTestHarness;

import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.Executors;

/**
 * Harness for testing customized operators handling {@link Event}s in CDC pipeline.
 *
 * <p>In addition to regular operator context and lifecycle management, this test harness also wraps
 * {@link TestingSchemaRegistryGateway} into the context of tested operator, in order to support
 * testing operators that have interaction with {@link SchemaCoordinator} via {@link
 * SchemaEvolutionClient}.
 *
 * @param <OP> Type of the operator
 * @param <E> Type of the event emitted by the operator
 */
public class DistributedEventOperatorTestHarness<
                OP extends AbstractStreamOperator<E>, E extends Event>
        extends BaseEventOperatorTestHarness<OP, E> {

    private final SchemaCoordinator schemaCoordinator;
    private final TestingSchemaRegistryGateway schemaRegistryGateway;

    public DistributedEventOperatorTestHarness(OP operator, int numOutputs) {
        this(operator, numOutputs, Duration.ofSeconds(3), Duration.ofMinutes(3));
    }

    public DistributedEventOperatorTestHarness(
            OP operator, int numOutputs, Duration applyDuration, Duration rpcTimeout) {
        super(operator, numOutputs);
        this.schemaCoordinator =
                new SchemaCoordinator(
                        "SchemaRegistry",
                        this.mockedContext,
                        Executors.newFixedThreadPool(1),
                        new CollectingMetadataApplier(applyDuration),
                        new ArrayList<>(),
                        SchemaChangeBehavior.LENIENT,
                        rpcTimeout);
        this.schemaRegistryGateway = new TestingSchemaRegistryGateway(this.schemaCoordinator);
    }

    public void open() throws Exception {
        schemaCoordinator.start();
        initializeOperator(this.schemaRegistryGateway);
        this.getOperator().open();
    }

    public void registerTableSchema(TableId tableId, Schema schema) {
        schemaCoordinator.emplaceOriginalSchema(tableId, 0, schema);
        schemaCoordinator.emplaceEvolvedSchema(tableId, schema);
    }

    public Schema getLatestEvolvedSchema(TableId tableId) throws Exception {
        return ((GetEvolvedSchemaResponse)
                        CoordinationResponseUtils.unwrap(
                                schemaCoordinator
                                        .handleCoordinationRequest(
                                                new GetEvolvedSchemaRequest(
                                                        tableId,
                                                        GetEvolvedSchemaRequest
                                                                .LATEST_SCHEMA_VERSION))
                                        .get()))
                .getSchema()
                .orElse(null);
    }

    public boolean isJobFailed() {
        return mockedContext.isJobFailed();
    }

    public Throwable getJobFailureCause() {
        return mockedContext.getFailureCause();
    }

    @Override
    public void close() throws Exception {
        this.getOperator().close();
    }
}
