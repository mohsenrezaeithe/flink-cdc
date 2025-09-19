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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.cdc.common.annotation.Internal;
import org.apache.flink.cdc.common.event.ChangeEvent;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.FlushEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEventType;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.runtime.operators.sink.SchemaEvolutionClient;
import org.apache.flink.cdc.runtime.operators.sink.exception.SinkWrapperException;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;

import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

/**
 * An operator that processes records to be written into a {@link
 * org.apache.flink.api.connector.sink2.Sink}.
 *
 * <p>The operator is a proxy of SinkWriterOperator in Flink.
 *
 * <p>The operator is always part of a sink pipeline and is the first operator.
 *
 * @param <CommT> the type of the committable (to send to downstream operators)
 */
@Internal
public class DataSinkWriterOperator<CommT> extends SinkWriterOperator<Event, CommT> {

    private SchemaEvolutionClient schemaEvolutionClient;

    private final OperatorID schemaOperatorID;

    /**
     * The internal {@link SinkWriter} of flinkWriterOperator, obtained it through reflection to
     * deal with {@link FlushEvent}.
     */
    private SinkWriter<Event> copySinkWriter;

    /** A set of {@link TableId} that already processed {@link CreateTableEvent}. */
    private final Set<TableId> processedTableIds = new HashSet<>();

    public DataSinkWriterOperator(
            StreamOperatorParameters<CommittableMessage<CommT>> parameters,
            Sink<Event> sink,
            ProcessingTimeService processingTimeService,
            MailboxExecutor mailboxExecutor,
            OperatorID schemaOperatorID) {
        super(parameters, sink, processingTimeService, mailboxExecutor);
        this.schemaOperatorID = schemaOperatorID;
    }

    @Override
    public void setup(
            StreamTask<?, ?> containingTask,
            StreamConfig config,
            Output<StreamRecord<CommittableMessage<CommT>>> output) {
        super.setup(containingTask, config, output);
        schemaEvolutionClient =
                new SchemaEvolutionClient(
                        containingTask.getEnvironment().getOperatorCoordinatorEventGateway(),
                        schemaOperatorID);
    }

    @Override
    public void open() throws Exception {
        super.open();
        copySinkWriter = getFieldValue("sinkWriter");
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        schemaEvolutionClient.registerSubtask(
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask());
        super.initializeState(context);
    }

    @Override
    public void processElement(StreamRecord<Event> element) throws Exception {
        Event event = element.getValue();

        try {
            // FlushEvent triggers flush
            if (event instanceof FlushEvent) {
                handleFlushEvent(((FlushEvent) event));
                return;
            }

            // CreateTableEvent marks the table as processed directly
            if (event instanceof CreateTableEvent) {
                processedTableIds.add(((CreateTableEvent) event).tableId());
                super.processElement(element);
                return;
            }

            // Check if the table is processed before emitting all other events, because we have to
            // make
            // sure that sink have a view of the full schema before processing any change events,
            // including schema changes.
            ChangeEvent changeEvent = (ChangeEvent) event;
            if (!processedTableIds.contains(changeEvent.tableId())) {
                emitLatestSchema(changeEvent.tableId());
                processedTableIds.add(changeEvent.tableId());
            }
            processedTableIds.add(changeEvent.tableId());
            super.processElement(element);
        } catch (Exception e) {
            throw new SinkWrapperException(event, e);
        }
    }

    // ----------------------------- Helper functions -------------------------------

    private void handleFlushEvent(FlushEvent event) throws Exception {
        copySinkWriter.flush(false);
        if (event.getSchemaChangeEventType() != SchemaChangeEventType.CREATE_TABLE
                && event.getSchemaChangeEventType() != SchemaChangeEventType.DROP_TABLE) {
            event.getTableIds().stream()
                    .filter(tableId -> !processedTableIds.contains(tableId))
                    .forEach(
                            tableId -> {
                                LOG.info("Table {} has not been processed", tableId);
                                try {
                                    emitLatestSchema(tableId);
                                } catch (Exception e) {
                                    throw new RuntimeException(e);
                                }
                                processedTableIds.add(tableId);
                            });
        }
        schemaEvolutionClient.notifyFlushSuccess(
                getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(),
                event.getSourceSubTaskId());
    }

    private void emitLatestSchema(TableId tableId) throws Exception {
        Optional<Schema> schema = schemaEvolutionClient.getLatestEvolvedSchema(tableId);
        if (schema.isPresent()) {
            // request and process CreateTableEvent because SinkWriter need to retrieve
            // Schema to deserialize RecordData after resuming job.
            super.processElement(new StreamRecord<>(new CreateTableEvent(tableId, schema.get())));
            processedTableIds.add(tableId);
        } else {
            throw new RuntimeException(
                    "Could not find schema message from SchemaRegistry for " + tableId);
        }
    }

    // -------------------------- Reflection helper functions --------------------------

    /**
     * Finds a field by name from its declaring class. This also searches for the field in super
     * classes.
     *
     * @param fieldName the name of the field to find.
     * @return the Object value of this field.
     */
    @SuppressWarnings("unchecked")
    private <T> T getFieldValue(String fieldName) throws IllegalAccessException {
        Class<?> clazz = super.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return (T) field.get(this);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new RuntimeException("failed to get sinkWriter");
    }
}
