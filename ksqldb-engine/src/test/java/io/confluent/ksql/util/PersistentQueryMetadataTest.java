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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.confluent.ksql.execution.plan.ExecutionStep;
import io.confluent.ksql.execution.streams.materialization.MaterializationProvider;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.MaterializationProviderBuilderFactory;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.PhysicalSchema;
import io.confluent.ksql.schema.query.QuerySchemas;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
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
  private Consumer<QueryMetadata> closeCallback;
  @Mock
  private QueryErrorClassifier queryErrorClassifier;
  @Mock
  private ExecutionStep<?> physicalPlan;
  @Mock
  private ProcessingLogger processingLogger;

  private PersistentQueryMetadata query;

  @Before
  public void setUp()  {
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(kafkaStreams);
    when(physicalSchema.logicalSchema()).thenReturn(mock(LogicalSchema.class));
    when(materializationProviderBuilder.apply(kafkaStreams))
        .thenReturn(Optional.of(materializationProvider));

    query = new PersistentQueryMetadata(
        SQL,
        physicalSchema,
        Collections.emptySet(),
        sinkDataSource,
        EXECUTION_PLAN,
        QUERY_ID,
        Optional.of(materializationProviderBuilder),
        APPLICATION_ID,
        topology,
        kafkaStreamsBuilder,
        schemas,
        props,
        overrides,
        closeCallback,
        CLOSE_TIMEOUT,
        queryErrorClassifier,
        physicalPlan,
        10,
        processingLogger
    );

    query.initialize();
  }

  @Test
  public void shouldCloseKafkaStreamsOnStop() {
    // When:
    query.stop();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams);
    inOrder.verify(kafkaStreams).close(any());
    inOrder.verifyNoMoreInteractions();
  }

  @Test
  @SuppressWarnings("deprecation") // https://github.com/confluentinc/ksql/issues/6639
  public void shouldRestartKafkaStreams() {
    final KafkaStreams newKafkaStreams = mock(KafkaStreams.class);
    final MaterializationProvider newMaterializationProvider = mock(MaterializationProvider.class);

    // Given:
    when(kafkaStreamsBuilder.build(any(), any())).thenReturn(newKafkaStreams);
    when(materializationProviderBuilder.apply(newKafkaStreams))
        .thenReturn(Optional.of(newMaterializationProvider));

    // When:
    query.restart();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams, newKafkaStreams);
    inOrder.verify(kafkaStreams).close(any());
    inOrder.verify(newKafkaStreams).setUncaughtExceptionHandler(
        any(Thread.UncaughtExceptionHandler.class));
    inOrder.verify(newKafkaStreams).start();

    assertThat(query.getKafkaStreams(), is(newKafkaStreams));
    assertThat(query.getMaterializationProvider(), is(Optional.of(newMaterializationProvider)));
  }

  @Test
  public void shouldNotRestartIfQueryIsClosed() {
    // Given:
    query.close();

    // When:
    final Exception e = assertThrows(Exception.class, () -> query.restart());

    // Then:
    assertThat(e.getMessage(), containsString("is already closed, cannot restart."));
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
    query.uncaughtHandler(thread, error);

    // Then:
    verify(processingLogger).error(errorMessageCaptor.capture());
    assertThat(
        KafkaStreamsThreadError.of(
            "Unhandled exception caught in streams thread", thread, error),
        is(errorMessageCaptor.getValue()));
  }
}
