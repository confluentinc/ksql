/*
 * Copyright 2018 Confluent Inc.
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
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.base.Ticker;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.logging.processing.MeteredProcessingLoggerFactory;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.query.QueryLogger;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryError;
import io.confluent.ksql.query.QueryError.Type;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryMetadataTest {

  private static final long RETRY_BACKOFF_INITIAL_MS = 1;
  private static final long RETRY_BACKOFF_MAX_MS = 10;
  private static final String QUERY_APPLICATION_ID = "Query1";
  private static final QueryId QUERY_ID = new QueryId("queryId");
  private static final LogicalSchema SOME_SCHEMA = LogicalSchema.builder()
      .keyColumn(SystemColumns.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("f0"), SqlTypes.STRING)
      .build();

  private static final Set<SourceName> SOME_SOURCES = ImmutableSet.of(SourceName.of("s1"), SourceName.of("s2"));
  private static final Long closeTimeout = KsqlConfig.KSQL_SHUTDOWN_TIMEOUT_MS_DEFAULT;

  @Mock
  private KafkaStreamsBuilder kafkaStreamsBuilder;
  @Mock
  private Topology topoplogy;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private QueryMetadataImpl.Listener listener;
  @Mock
  private QueryErrorClassifier classifier;
  @Captor
  private ArgumentCaptor<KafkaStreams.StateListener> streamsListenerCaptor;
  @Mock
  private Ticker ticker;
  @Mock
  private MeteredProcessingLoggerFactory loggerFactory;

  private QueryMetadataImpl query;

  @Before
  public void setup() {
    when(kafkaStreamsBuilder.build(topoplogy, Collections.emptyMap())).thenReturn(kafkaStreams);
    when(classifier.classify(any())).thenReturn(Type.UNKNOWN);
    when(kafkaStreams.state()).thenReturn(State.NOT_RUNNING);

    query = new QueryMetadataImpl(
        "foo",
        SOME_SCHEMA,
        SOME_SOURCES,
        "bar",
        QUERY_APPLICATION_ID,
        topoplogy,
        kafkaStreamsBuilder,
        Collections.emptyMap(),
        Collections.emptyMap(),
        closeTimeout,
        QUERY_ID,
        classifier,
        10,
        0L,
        0L,
        listener,
        loggerFactory
    ){
    };
    query.initialize();
  }

  @Test
  public void shouldSetInitialStateWhenStarted() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.CREATED);

    // When:
    query.start();

    // Then:
    verify(listener).onStateChange(query, State.CREATED, State.CREATED);
  }

  @Test
  public void shouldConnectAnyListenerToStreamAppOnInitialize() {
    // When:
    verify(kafkaStreams).setStateListener(streamsListenerCaptor.capture());
    final KafkaStreams.StateListener streamsListener = streamsListenerCaptor.getValue();
    streamsListener.onChange(State.CREATED, State.RUNNING);

    // Then:
    verify(listener).onStateChange(query, State.CREATED, State.RUNNING);
  }

  @Test
  public void shouldNotifyAnyListenerOnClose() {
    // When:
    query.close();

    // Then:
    verify(listener).onClose(query);
  }

  @Test
  public void shouldReturnStreamState() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.PENDING_SHUTDOWN);

    // When:
    final String state = query.getState().toString();

    // Then:
    assertThat(state, is("PENDING_SHUTDOWN"));
  }

  @Test
  public void shouldCloseKStreamsAppOnCloseThenCloseCallback() {
    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams, listener);
    inOrder.verify(kafkaStreams).close(Duration.ofMillis(closeTimeout));
    inOrder.verify(listener).onClose(query);
  }

  @Test
  public void shouldCleanUpKStreamsAppAfterCloseOnClose() {
    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams);
    inOrder.verify(kafkaStreams).close(Duration.ofMillis(closeTimeout));
    inOrder.verify(kafkaStreams).cleanUp();
  }

  @Test
  public void shouldSkipCleanUpKStreamsAppAfterCloseOnCloseIfRunning() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.RUNNING);

    // When:
    query.close();

    // Then:
    final InOrder inOrder = inOrder(kafkaStreams);
    inOrder.verify(kafkaStreams).close(Duration.ofMillis(closeTimeout));
    inOrder.verify(kafkaStreams, never()).cleanUp();
  }

  @Test
  public void shouldReturnSources() {
    assertThat(query.getSourceNames(), is(SOME_SOURCES));
  }

  @Test
  public void shouldReturnSchema() {
    assertThat(query.getLogicalSchema(), is(SOME_SCHEMA));
  }

  @Test
  public void shouldNotifyQueryStateListenerOnError() {
    // Given:
    when(classifier.classify(any())).thenReturn(Type.USER);

    // When:
    query.uncaughtHandler(new RuntimeException("oops"));

    // Then:
    verify(listener).onError(same(query), argThat(q -> q.getType().equals(Type.USER)));
  }

  @Test
  public void shouldNotifyQueryStateListenerWithRecursiveClassification() {
    final Exception e = new Exception();

    // Given:
    when(classifier.classify(eq(e))).thenReturn(Type.USER);

    // When:
    query.uncaughtHandler(new Exception("oops", e));

    // Then:
    verify(listener).onError(same(query), argThat(q -> q.getType().equals(Type.USER)));
  }

  @Test
  public void shouldNotifyQueryStateListenerWithSelfReferencingException() {
    final Exception e = new Exception() {
      @Override
      public synchronized Throwable getCause() {
        return this;
      }
    };

    // When:
    query.uncaughtHandler(new Exception("oops", e));

    // Then:
    verify(listener).onError(same(query), argThat(q -> q.getType().equals(Type.UNKNOWN)));
  }

  @Test
  public void shouldNotifyQueryStateListenerOnErrorEvenIfClassifierFails() {
    // Given:
    final RuntimeException thrown = new RuntimeException("bar");
    when(classifier.classify(any())).thenThrow(thrown);

    // When:
    query.uncaughtHandler(new RuntimeException("foo"));


    // Then:
    verify(listener).onError(same(query), argThat(q -> q.getType().equals(Type.UNKNOWN)));
  }

  @Test
  public void queryLoggerShouldReceiveStatementsWhenUncaughtHandler() {
    try (MockedStatic<QueryLogger> logger = Mockito.mockStatic(QueryLogger.class)) {
      query.uncaughtHandler(new RuntimeException("foo"));

      logger.verify(() ->
          QueryLogger.error("Uncaught exception in query java.lang.RuntimeException: foo",
          "foo"), times(1));
    }
  }

  @Test
  public void shouldReturnPersistentQueryTypeByDefault() {
    assertThat(query.getQueryType(), is(KsqlQueryType.PERSISTENT));
  }

  @Test
  public void shouldRetryEventStartWithInitialValues() {
    // Given:
    final long now = 20;
    when(ticker.read()).thenReturn(now);

    // When:
    final QueryMetadataImpl.RetryEvent retryEvent = new QueryMetadataImpl.RetryEvent(
            QUERY_ID,
            RETRY_BACKOFF_INITIAL_MS,
            RETRY_BACKOFF_MAX_MS,
            ticker
    );

    // Then:
    assertThat(retryEvent.getNumRetries("thread-name"), is(0));
    assertThat(retryEvent.nextRestartTimeMs(), is(now + RETRY_BACKOFF_INITIAL_MS));
  }

  @Test
  public void shouldRetryEventRestartAndIncrementBackoffTime() {
    // Given:
    final long now = 20;
    when(ticker.read()).thenReturn(now);

    // When:
    final QueryMetadataImpl.RetryEvent retryEvent = new QueryMetadataImpl.RetryEvent(
            QUERY_ID,
            RETRY_BACKOFF_INITIAL_MS,
            RETRY_BACKOFF_MAX_MS,
            ticker
    );

    retryEvent.backOff("thread-name");
    retryEvent.backOff("thread-name");
    retryEvent.backOff("thread-name-2");
    final int numBackOff = 3;

    // Then:
    assertThat(retryEvent.getNumRetries("thread-name"), is(2));
    assertThat(retryEvent.getNumRetries("thread-name-2"), is(1));
    assertThat(retryEvent.nextRestartTimeMs(), is(now + (RETRY_BACKOFF_INITIAL_MS * (int)(Math.pow(2, numBackOff)))));
  }

  @Test
  public void shouldRetryEventRestartAndNotExceedBackoffMaxTime() {
    // Given:
    final long now = 20;
    when(ticker.read()).thenReturn(now);

    // When:
    final QueryMetadataImpl.RetryEvent retryEvent = new QueryMetadataImpl.RetryEvent(
            QUERY_ID,
            RETRY_BACKOFF_INITIAL_MS,
            RETRY_BACKOFF_MAX_MS,
            ticker
    );
    retryEvent.backOff("thread-name");
    retryEvent.backOff("thread-name");
    retryEvent.backOff("thread-name");
    retryEvent.backOff("thread-name");
    retryEvent.backOff("thread-name");
    retryEvent.backOff("thread-name");

    // Then:
    assertThat(retryEvent.getNumRetries("thread-name"), is(6));
    assertThat(retryEvent.nextRestartTimeMs(), lessThanOrEqualTo(now + RETRY_BACKOFF_MAX_MS));
  }

  @Test
  public void shouldEvictBasedOnTime() {
    // Given:
    final QueryMetadataImpl.TimeBoundedQueue queue = new QueryMetadataImpl.TimeBoundedQueue(Duration.ZERO, 1);
    queue.add(new QueryError(System.currentTimeMillis(), "test", Type.SYSTEM));

    //Then:
    assertThat(queue.toImmutableList().size(), is(0));
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
