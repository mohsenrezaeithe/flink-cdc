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
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.SchemaChangeEventTypeFamily;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.schema.common.CoordinationResponseUtils;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetEvolvedSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaRequest;
import org.apache.flink.cdc.runtime.operators.schema.common.event.GetOriginalSchemaResponse;
import org.apache.flink.cdc.runtime.operators.schema.regular.SchemaCoordinator;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeRequest;
import org.apache.flink.cdc.runtime.operators.schema.regular.event.SchemaChangeResponse;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.testutils.schema.CollectingMetadataApplier;
import org.apache.flink.cdc.runtime.testutils.schema.TestingSchemaRegistryGateway;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BaseEventOperatorTestHarness;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

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
public class RegularEventOperatorTestHarness<OP extends AbstractStreamOperator<E>, E extends Event>
        extends BaseEventOperatorTestHarness<OP, E> {

    private static final Duration DEFAULT_RPC_TIMEOUT =
            PipelineOptions.DEFAULT_SCHEMA_OPERATOR_RPC_TIMEOUT;

    private final SchemaCoordinator schemaRegistry;
    private final TestingSchemaRegistryGateway schemaRegistryGateway;

    private RegularEventOperatorTestHarness(
            OP operator,
            int numOutputs,
            Duration schemaEvolveDuration,
            Duration rpcTimeout,
            SchemaChangeBehavior behavior,
            Set<SchemaChangeEventType> enabledEventTypes,
            Set<SchemaChangeEventType> errorsOnEventTypes) {
        super(operator, numOutputs);
        schemaRegistry =
                new SchemaCoordinator(
                        "SchemaOperator",
                        this.mockedContext,
                        Executors.newFixedThreadPool(1),
                        new CollectingMetadataApplier(
                                schemaEvolveDuration, enabledEventTypes, errorsOnEventTypes),
                        new ArrayList<>(),
                        behavior,
                        rpcTimeout);
        schemaRegistryGateway = new TestingSchemaRegistryGateway(schemaRegistry);
    }

    public static <OP extends AbstractStreamOperator<E>, E extends Event>
            RegularEventOperatorTestHarness<OP, E> with(OP operator, int numOutputs) {
        return new RegularEventOperatorTestHarness<>(
                operator,
                numOutputs,
                null,
                DEFAULT_RPC_TIMEOUT,
                SchemaChangeBehavior.EVOLVE,
                Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet()),
                Collections.emptySet());
    }

    public static <OP extends AbstractStreamOperator<E>, E extends Event>
            RegularEventOperatorTestHarness<OP, E> withDuration(
                    OP operator, int numOutputs, Duration evolveDuration) {
        return new RegularEventOperatorTestHarness<>(
                operator,
                numOutputs,
                evolveDuration,
                DEFAULT_RPC_TIMEOUT,
                SchemaChangeBehavior.EVOLVE,
                Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet()),
                Collections.emptySet());
    }

    public static <OP extends AbstractStreamOperator<E>, E extends Event>
            RegularEventOperatorTestHarness<OP, E> withDurationAndBehavior(
                    OP operator,
                    int numOutputs,
                    Duration evolveDuration,
                    SchemaChangeBehavior behavior) {
        return new RegularEventOperatorTestHarness<>(
                operator,
                numOutputs,
                evolveDuration,
                DEFAULT_RPC_TIMEOUT,
                behavior,
                Arrays.stream(SchemaChangeEventTypeFamily.ALL).collect(Collectors.toSet()),
                Collections.emptySet());
    }

    public static <OP extends AbstractStreamOperator<E>, E extends Event>
            RegularEventOperatorTestHarness<OP, E> withDurationAndFineGrainedBehavior(
                    OP operator,
                    int numOutputs,
                    Duration evolveDuration,
                    SchemaChangeBehavior behavior,
                    Set<SchemaChangeEventType> enabledEventTypes) {
        return new RegularEventOperatorTestHarness<>(
                operator,
                numOutputs,
                evolveDuration,
                DEFAULT_RPC_TIMEOUT,
                behavior,
                enabledEventTypes,
                Collections.emptySet());
    }

    public static <OP extends AbstractStreamOperator<E>, E extends Event>
            RegularEventOperatorTestHarness<OP, E> withDurationAndFineGrainedBehaviorWithError(
                    OP operator,
                    int numOutputs,
                    Duration evolveDuration,
                    SchemaChangeBehavior behavior,
                    Set<SchemaChangeEventType> enabledEventTypes,
                    Set<SchemaChangeEventType> errorOnEventTypes) {

        return new RegularEventOperatorTestHarness<>(
                operator,
                numOutputs,
                evolveDuration,
                DEFAULT_RPC_TIMEOUT,
                behavior,
                enabledEventTypes,
                errorOnEventTypes);
    }

    public void open() throws Exception {
        schemaRegistry.start();
        initializeOperator(schemaRegistryGateway);
        this.getOperator().open();
    }

    public void registerTableSchema(TableId tableId, Schema schema) {
        schemaRegistry.emplaceOriginalSchema(tableId, schema);
        schemaRegistry.emplaceEvolvedSchema(tableId, schema);
    }

    public void registerOriginalSchema(TableId tableId, Schema schema) {
        schemaRegistry.emplaceOriginalSchema(tableId, schema);
    }

    public void registerEvolvedSchema(TableId tableId, Schema schema) {
        schemaRegistry.emplaceEvolvedSchema(tableId, schema);
    }

    public SchemaChangeResponse requestSchemaChangeEvent(TableId tableId, SchemaChangeEvent event)
            throws ExecutionException, InterruptedException {
        return CoordinationResponseUtils.unwrap(
                schemaRegistry
                        .handleCoordinationRequest(new SchemaChangeRequest(tableId, event, 0))
                        .get());
    }

    public Schema getLatestOriginalSchema(TableId tableId) throws Exception {
        return ((GetOriginalSchemaResponse)
                        CoordinationResponseUtils.unwrap(
                                schemaRegistry
                                        .handleCoordinationRequest(
                                                new GetOriginalSchemaRequest(
                                                        tableId,
                                                        GetOriginalSchemaRequest
                                                                .LATEST_SCHEMA_VERSION))
                                        .get()))
                .getSchema()
                .orElse(null);
    }

    public Schema getLatestEvolvedSchema(TableId tableId) throws Exception {
        return ((GetEvolvedSchemaResponse)
                        CoordinationResponseUtils.unwrap(
                                schemaRegistry
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
}
