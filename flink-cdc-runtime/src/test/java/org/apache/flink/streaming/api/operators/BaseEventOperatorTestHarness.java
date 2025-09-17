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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.FlushSuccessEvent;
import org.apache.flink.cdc.runtime.operators.schema.common.event.SinkWriterRegisterEvent;
import org.apache.flink.cdc.runtime.testutils.operators.MockedOperatorCoordinatorContext;
import org.apache.flink.cdc.runtime.testutils.schema.TestingSchemaRegistryGateway;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.operators.testutils.DummyEnvironment;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.RecordAttributes;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
import org.apache.flink.streaming.util.MockStreamConfig;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.SerializedValue;

import java.io.IOException;
import java.util.LinkedList;

/**
 * Base class for event operator test harnesses that provides common functionality for operator
 * initialization using reflection to access protected methods.
 *
 * @param <OP> Type of the operator
 * @param <E> Type of the event emitted by the operator
 */
public abstract class BaseEventOperatorTestHarness<
                OP extends AbstractStreamOperator<E>, E extends Event>
        implements AutoCloseable {

    public static final OperatorID SCHEMA_OPERATOR_ID = new OperatorID(15213L, 15513L);
    public static final OperatorID SINK_OPERATOR_ID = new OperatorID(15214L, 15514L);

    protected final OP operator;
    protected final int numOutputs;
    protected final LinkedList<StreamRecord<E>> outputRecords = new LinkedList<>();
    protected final MockedOperatorCoordinatorContext mockedContext;

    /**
     * Constructor for base test harness using Proxy Pattern.
     *
     * @param operator The operator to test
     * @param numOutputs Number of outputs for the operator
     */
    protected BaseEventOperatorTestHarness(OP operator, int numOutputs) {
        this.operator = operator;
        this.numOutputs = numOutputs;
        this.mockedContext =
                new MockedOperatorCoordinatorContext(
                        SCHEMA_OPERATOR_ID, Thread.currentThread().getContextClassLoader());
    }

    /**
     * Initialize the operator using reflection to access the protected setup method. This method is
     * common to all operator test harnesses.
     *
     * @param schemaRegistryGateway The schema registry gateway for the test
     * @throws Exception if initialization fails
     */
    protected void initializeOperator(TestingSchemaRegistryGateway schemaRegistryGateway)
            throws Exception {
        this.operator.setup(
                new MockStreamTask(schemaRegistryGateway),
                new MockStreamConfig(new Configuration(), this.numOutputs),
                new EventCollectingOutput<>(this.getOutputRecords(), schemaRegistryGateway));

        schemaRegistryGateway.sendOperatorEventToCoordinator(
                SINK_OPERATOR_ID, new SerializedValue<>(new SinkWriterRegisterEvent(0)));
    }

    /**
     * Get the output records collected during testing.
     *
     * @return LinkedList of output records
     */
    public LinkedList<StreamRecord<E>> getOutputRecords() {
        return outputRecords;
    }

    /** Clear the collected output records. */
    public void clearOutputRecords() {
        outputRecords.clear();
    }

    /** Get the original operator being tested. */
    public OP getOperator() {
        return this.operator;
    }

    public void close() throws Exception {
        this.getOperator().close();
    }

    // ---------------------------------------- Helper classes ---------------------------------

    /** Output implementation that collects events for testing. */
    private static class EventCollectingOutput<E extends Event> implements Output<StreamRecord<E>> {
        private final LinkedList<StreamRecord<E>> outputRecords;
        private final TestingSchemaRegistryGateway schemaRegistryGateway;

        public EventCollectingOutput(
                LinkedList<StreamRecord<E>> outputRecords,
                TestingSchemaRegistryGateway schemaRegistryGateway) {
            this.outputRecords = outputRecords;
            this.schemaRegistryGateway = schemaRegistryGateway;
        }

        @Override
        public void collect(StreamRecord<E> record) {
            this.outputRecords.add(record);
            Event event = record.getValue();
            if (event instanceof FlushEvent) {
                try {
                    this.schemaRegistryGateway.sendOperatorEventToCoordinator(
                            SINK_OPERATOR_ID, new SerializedValue<>(new FlushSuccessEvent(0, 0)));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public void emitWatermark(Watermark mark) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitWatermarkStatus(WatermarkStatus watermarkStatus) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitWatermark(WatermarkEvent watermarkEvent) {
            throw new UnsupportedOperationException();
        }

        @Override
        public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitLatencyMarker(LatencyMarker latencyMarker) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void emitRecordAttributes(RecordAttributes recordAttributes) {}

        @Override
        public void close() {}
    }

    /** Mock stream task for testing. */
    private static class MockStreamTask extends StreamTask<Event, AbstractStreamOperator<Event>> {
        protected MockStreamTask(TestingSchemaRegistryGateway schemaRegistryGateway)
                throws Exception {
            super(new SchemaRegistryCoordinatingEnvironment(schemaRegistryGateway));
        }

        @Override
        protected void init() {}
    }

    /** Mock environment that provides schema registry coordination. */
    private static class SchemaRegistryCoordinatingEnvironment extends DummyEnvironment {
        private final TestingSchemaRegistryGateway schemaRegistryGateway;

        public SchemaRegistryCoordinatingEnvironment(
                TestingSchemaRegistryGateway schemaRegistryGateway) {
            this.schemaRegistryGateway = schemaRegistryGateway;
        }

        @Override
        public TaskOperatorEventGateway getOperatorCoordinatorEventGateway() {
            return schemaRegistryGateway;
        }
    }
}
