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
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.internal.QueryStateListener;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.query.KafkaStreamsBuilder;
import io.confluent.ksql.query.QueryErrorClassifier;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.Topology;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class QueryMetadataTest {

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
  private QueryStateListener listener;
  @Mock
  private Consumer<QueryMetadata> closeCallback;
  private QueryMetadata query;

  @Before
  public void setup() {
    when(kafkaStreamsBuilder.build(topoplogy, Collections.emptyMap())).thenReturn(kafkaStreams);

    query = new QueryMetadata(
        "foo",
        SOME_SCHEMA,
        SOME_SOURCES,
        "bar",
        QUERY_APPLICATION_ID,
        topoplogy,
        kafkaStreamsBuilder,
        Collections.emptyMap(),
        Collections.emptyMap(),
        closeCallback,
        closeTimeout,
        QUERY_ID, QueryErrorClassifier.DEFAULT_CLASSIFIER,
        10){
    };
  }

  @Test
  public void shouldSetInitialStateWhenListenerAdd() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.CREATED);

    // When:
    query.setQueryStateListener(listener);

    // Then:
    verify(listener).onChange(State.CREATED, State.CREATED);
  }

  @Test
  public void shouldGetUptimeFromStateListener() {
    // Given:
    when(kafkaStreams.state()).thenReturn(State.RUNNING);
    when(listener.uptime()).thenReturn(5L);

    // When:
    query.setQueryStateListener(listener);

    // Then:
    assertThat(query.uptime(), is(5L));
  }

  @Test
  public void shouldReturnZeroUptimeIfNoStateListenerSet() {
    // When/Then:
    assertThat(query.uptime(), is(0L));
  }

  @Test
  public void shouldConnectAnyListenerToStreamAppOnStart() {
    // Given:
    query.setQueryStateListener(listener);

    // When:
    query.start();

    // Then:
    verify(kafkaStreams).setStateListener(listener);
  }

  @Test
  public void shouldCloseAnyListenerOnClose() {
    // Given:
    query.setQueryStateListener(listener);

    // When:
    query.close();

    // Then:
    verify(listener).close();
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
    final InOrder inOrder = inOrder(kafkaStreams, closeCallback);
    inOrder.verify(kafkaStreams).close(Duration.ofMillis(closeTimeout));
    inOrder.verify(closeCallback).accept(query);
  }

  @Test
  public void shouldNotCallCloseCallbackOnStop() {
    // When:
    query.stop();

    // Then:
    verifyNoMoreInteractions(closeCallback);
  }

  @Test
  public void shouldCallKafkaStreamsCloseOnStop() {
    // When:
    query.stop();

    // Then:
    verify(kafkaStreams).close(Duration.ofMillis(closeTimeout));
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
  public void shouldNotCleanUpKStreamsAppOnStop() {
    // When:
    query.stop();

    // Then:
    verify(kafkaStreams, never()).cleanUp();
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
  public void shouldReturnPersistentQueryTypeByDefault() {
    assertThat(query.getQueryType(), is(KsqlQueryType.PERSISTENT));
  }
}
