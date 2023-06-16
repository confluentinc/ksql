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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.execution.scalablepush.ScalablePushRegistry;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.MaterializationProviderBuilderFactory;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import io.confluent.ksql.util.QueryMetadata.Listener;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PersistentQueryMetadataTest {
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
  private QueryErrorClassifier queryErrorClassifier;
  @Mock
  private ExecutionStep<?> physicalPlan;
  @Mock
  private ProcessingLogger processingLogger;
  @Mock
  private Listener listener;
  @Mock
  private ScalablePushRegistry scalablePushRegistry;

  private PersistentQueryMetadata query;

  @Before
  public void setUp()  {
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(kafkaStreams);
    when(physicalSchema.logicalSchema()).thenReturn(mock(LogicalSchema.class));
    when(materializationProviderBuilder.apply(kafkaStreams, topology))
        .thenReturn(Optional.of(materializationProvider));
    when(kafkaStreams.state()).thenReturn(State.NOT_RUNNING);

    query = new PersistentQueryMetadataImpl(
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
        Optional.of(scalablePushRegistry)
    );

    query.initialize();
  }

  @Test
  public void shouldReturnInsertQueryType() {
    // Given
    final PersistentQueryMetadata query = new PersistentQueryMetadataImpl(
        KsqlConstants.PersistentQueryType.INSERT,
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
        Optional.empty()
    );

    // When/Then
    assertThat(query.getPersistentQueryType(), is(KsqlConstants.PersistentQueryType.INSERT));
  }

  @Test
  public void shouldReturnCreateAsQueryType() {
    // Given
    final PersistentQueryMetadata query = new PersistentQueryMetadataImpl(
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
        Optional.empty()
    );

    // When/Then
    assertThat(query.getPersistentQueryType(), is(KsqlConstants.PersistentQueryType.CREATE_AS));
  }

  @Test
  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
  public void shouldCloseKafkaStreamsOnStop() {
    // When:
    query.stop();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams);
    inOrder.verify(kafkaStreams).close(any());
    inOrder.verify(kafkaStreams).state();
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
    verify(kafkaStreams).close(Duration.ofMillis(CLOSE_TIMEOUT));
  }

  @Test
  public void shouldNotCleanUpKStreamsAppOnStop() {
    // When:
    query.stop();

    // Then:
    verify(kafkaStreams, never()).cleanUp();
  }

  @Test
  public void shouldCallProcessingLoggerOnError() {
    // Given:
    final Thread thread = mock(Thread.class);
    final Throwable error = mock(Throwable.class);
    final ArgumentCaptor<ProcessingLogger.ErrorMessage> errorMessageCaptor =
        ArgumentCaptor.forClass(ProcessingLogger.ErrorMessage.class);
    when(queryErrorClassifier.classify(error)).thenReturn(QueryError.Type.SYSTEM);

    // When:
    query.uncaughtHandler(error);

    // Then:
    verify(processingLogger).error(errorMessageCaptor.capture());
    assertThat(
        KafkaStreamsThreadError.of(
            "Unhandled exception caught in streams thread", thread, error),
        is(errorMessageCaptor.getValue()));
  }
}
