/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.util;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.materialization.MaterializationInfo;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.logging.processing.MeteredProcessingLoggerFactory;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.query.MaterializationProviderBuilderFactory;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.serde.KeyFormat;
import io.confluent.ksql.util.QueryMetadata.Listener;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class BinPackedPersistentQueryMetadataImplTest {
    private static final String SQL = "sql";
    private static final String EXECUTION_PLAN = "execution plan";
    private static final QueryId QUERY_ID = new QueryId("queryId");
    private static final String APPLICATION_ID = "applicationId";

    @Mock
    private PhysicalSchema physicalSchema;
    @Mock
    private DataSource sinkDataSource;
    @Mock
    private NamedTopology topology;
    @Mock
    private QuerySchemas schemas;
    @Mock
    private Map<String, Object> overrides;
    @Mock
    private ExecutionStep<?> physicalPlan;
    @Mock
    private ProcessingLogger processingLogger;
    @Mock
    private Listener listener;
    @Mock
    private SharedKafkaStreamsRuntimeImpl sharedKafkaStreamsRuntimeImpl;
    @Mock
    private Map<String, Object> streamsProperties;
    @Mock
    private Optional<ScalablePushRegistry> scalablePushRegistry;
    @Mock
    private MaterializationProviderBuilderFactory
        materializationProviderBuilderFactory;
    @Mock
    private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper;
    @Mock
    private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper2;
    @Mock
    private QueryContext.Stacker stacker;
    @Mock
    private KeyFormat keyFormat;
    @Mock
    private MaterializationInfo materializationInfo;
    @Mock
    private MaterializationProviderBuilderFactory.MaterializationProviderBuilder materializationProviderBuilder;
    @Mock
    private MeteredProcessingLoggerFactory loggerFactory;

    private PersistentQueryMetadata query;

    @Before
    public void setUp()  {
        query = new BinPackedPersistentQueryMetadataImpl(
            KsqlConstants.PersistentQueryType.CREATE_AS,
            SQL,
            physicalSchema,
            ImmutableSet.of(),
            EXECUTION_PLAN,
            APPLICATION_ID,
            topology,
            sharedKafkaStreamsRuntimeImpl,
            schemas,
            overrides,
            QUERY_ID,
            Optional.of(materializationInfo),
            materializationProviderBuilderFactory,
            physicalPlan,
            processingLogger,
            Optional.of(sinkDataSource),
            listener,
            scalablePushRegistry,
            (runtime) -> topology,
            keyFormat,
            loggerFactory);

        query.initialize();
        when(materializationProviderBuilderFactory.materializationProviderBuilder(
            any(), any(), any(), any(), any(), any()))
            .thenReturn(materializationProviderBuilder);
    }

    @Test
    public void shouldGetStreamsFreshForMaterialization() {
        when(sharedKafkaStreamsRuntimeImpl.getKafkaStreams())
            .thenReturn(kafkaStreamsNamedTopologyWrapper)
            .thenReturn(kafkaStreamsNamedTopologyWrapper2);
        when(materializationProviderBuilder.apply(any(KafkaStreams.class), any())).thenReturn(Optional.empty());
        query.getMaterialization(query.getQueryId(), stacker);
        query.getMaterialization(query.getQueryId(), stacker);

        verify(materializationProviderBuilder).apply(eq(kafkaStreamsNamedTopologyWrapper), any());
        verify(materializationProviderBuilder).apply(eq(kafkaStreamsNamedTopologyWrapper2), any());
    }

    @Test
    @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
    public void shouldCloseKafkaStreamsOnStop() {
        // When:
        query.stop();

        // Then:
        final InOrder inOrder = inOrder(sharedKafkaStreamsRuntimeImpl);
        inOrder.verify(sharedKafkaStreamsRuntimeImpl).stop(QUERY_ID, false);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldNotCallCloseCallbackOnStop() {
        // When:
        query.stop();

        // Then:
        verify(listener, times(0)).onClose(query);
    }

    @Test
    public void shouldCallKafkaStreamsCloseOnStop() {
        // When:
        query.stop();

        // Then:
        verify(sharedKafkaStreamsRuntimeImpl).stop(QUERY_ID, false);
    }

    @Test
    public void shouldCloseProcessingLoggers() {
        // Given:
        final ProcessingLogger processingLogger1 = mock(ProcessingLogger.class);
        final ProcessingLogger processingLogger2 = mock(ProcessingLogger.class);
        when(loggerFactory.getLoggersWithPrefix(QUERY_ID.toString())).thenReturn(Arrays.asList(processingLogger1, processingLogger2));

        // When:
        query.close();

        // Then:
        verify(processingLogger1).close();
        verify(processingLogger2).close();
    }
}
