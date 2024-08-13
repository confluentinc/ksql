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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.logging.processing.MeteredProcessingLoggerFactory;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.MaterializationProviderBuilderFactory;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.util.QueryMetadata.Listener;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SandboxedPersistentQueryMetadataImplTest {
  private static final String SQL = "sql";
  private static final String EXECUTION_PLAN = "execution plan";
  private static final QueryId QUERY_ID = new QueryId("queryId");
  private static final String APPLICATION_ID = "applicationId";
  private static final long CLOSE_TIMEOUT = 10L;

  @Mock
  private KafkaStreamsBuilder kafkaStreamsBuilder;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private PhysicalSchema physicalSchema;
  @Mock
  private DataSource sinkDataSource;
  @Mock
  private MaterializationProviderBuilderFactory.MaterializationProviderBuilder
      materializationProviderBuilder;
  @Mock
  private MaterializationProvider materializationProvider;
  @Mock
  private Topology topology;
  @Mock
  private QuerySchemas schemas;
  @Mock
  private Map<String, Object> props;
  @Mock
  private Map<String, Object> overrides;
  @Mock
  private Consumer<QueryMetadata> closeCallback;
  @Mock
  private QueryErrorClassifier queryErrorClassifier;
  @Mock
  private ExecutionStep<?> physicalPlan;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private Listener listener;
  @Mock
  private Listener sandboxListener;
  @Mock
  private MeteredProcessingLoggerFactory processingLoggerFactory;

  private SandboxedPersistentQueryMetadataImpl sandbox;

  @Before
  public void setUp() {
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(kafkaStreams);
    when(physicalSchema.logicalSchema()).thenReturn(mock(LogicalSchema.class));
    when(materializationProviderBuilder.apply(kafkaStreams, topology))
        .thenReturn(Optional.of(materializationProvider));

    final PersistentQueryMetadataImpl query = new PersistentQueryMetadataImpl(
        KsqlConstants.PersistentQueryType.CREATE_AS,
        SQL,
        physicalSchema,
        Collections.emptySet(),
        Optional.of(sinkDataSource),
        EXECUTION_PLAN,
        QUERY_ID,
        Optional.of(materializationProviderBuilder),
        APPLICATION_ID,
        topology,
        kafkaStreamsBuilder,
        schemas,
        props,
        overrides,
        CLOSE_TIMEOUT,
        queryErrorClassifier,
        physicalPlan,
        10,
        processingLogger,
        0L,
        0L,
        listener,
        Optional.empty(),
        processingLoggerFactory
    );

    query.initialize();

    sandbox = SandboxedPersistentQueryMetadataImpl.of(query, sandboxListener);
    reset(kafkaStreams);
  }

  @Test
  public void shouldNotStartKafkaStreamsOnStart() {
    // When:
    sandbox.start();

    // Then:
    verifyNoMoreInteractions(kafkaStreams);
  }

  @Test
  public void shouldNotCloseKafkaStreamsOnStop() {
    // When:
    sandbox.stop();

    // Then:
    verifyNoMoreInteractions(kafkaStreams);
  }

  @Test
  public void shouldNotCloseKafkaStreamsOnClose() {
    // When:
    sandbox.close();

    // Then:
    verifyNoMoreInteractions(kafkaStreams);
  }

  @Test
  public void shouldNotCloseStateListenerOnClose() {
    // When:
    sandbox.stop();

    // Then:
    verifyNoMoreInteractions(closeCallback);
  }
}
