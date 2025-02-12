/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams.materialization.ks;

import static org.apache.kafka.streams.KafkaStreams.State.NOT_RUNNING;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;
import static org.apache.kafka.streams.state.QueryableStoreTypes.sessionStore;
import static org.apache.kafka.streams.state.QueryableStoreTypes.windowStore;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.internals.namedtopology.KafkaStreamsNamedTopologyWrapper;
import org.apache.kafka.streams.processor.internals.namedtopology.NamedTopologyStoreQueryParameters;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.List;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@RunWith(MockitoJUnitRunner.class)
public class KsStateStoreTest {

  private static final String STORE_NAME = "someStore";
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .keyColumn(ColumnName.of("v0"), SqlTypes.BIGINT)
      .build();

  @Mock
  private KafkaStreamsNamedTopologyWrapper kafkaStreamsNamedTopologyWrapper;
  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KsqlConfig ksqlConfig;

  @Captor
  ArgumentCaptor<StoreQueryParameters<?>> storeQueryParamCaptor;

  private KsStateStore store;

  @Before
  public void setUp() {
    store = new KsStateStore(STORE_NAME, kafkaStreams, SCHEMA, ksqlConfig, "queryId");
    when(kafkaStreams.state()).thenReturn(State.RUNNING);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KafkaStreams.class, kafkaStreams)
        .setDefault(LogicalSchema.class, SCHEMA)
        .setDefault(KsqlConfig.class, ksqlConfig)
        .testConstructors(KsStateStore.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldUseNamedTopologyWhenSharedRuntimeIsEnabled() {
    // Given:
    final QueryableStoreType<ReadOnlySessionStore<String, Long>> storeType =
        QueryableStoreTypes.sessionStore();
    final KsStateStore store =
        new KsStateStore(STORE_NAME, kafkaStreamsNamedTopologyWrapper, SCHEMA, ksqlConfig, "queryId");

    // When:
    store.store(storeType, 0);

    // Then:
    verify(kafkaStreamsNamedTopologyWrapper).store(storeQueryParamCaptor.capture());
    List<StoreQueryParameters<?>> keys = storeQueryParamCaptor.getAllValues();
    assertThat(keys.get(0), instanceOf(NamedTopologyStoreQueryParameters.class));
  }

  @Test
  public void shouldNotAwaitRunning() {
    // Given:
    final QueryableStoreType<ReadOnlySessionStore<String, Long>> storeType =
        QueryableStoreTypes.sessionStore();

    // When:
    store.store(storeType, 0);

    // Then:
    verify(kafkaStreams, never()).state();
  }

  @Test
  public void shouldThrowIfNotRunningAfterFailedToGetStore() {
    // Given:
    when(kafkaStreams.state())
        .thenReturn(RUNNING)
        .thenReturn(NOT_RUNNING);
    when(kafkaStreams.store(any())).thenThrow(new IllegalStateException());

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> store.store(sessionStore(), 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString("State store currently unavailable: someStore"));
  }

  @Test
  public void shouldGetStoreOnceRunning() {
    // When:
    store.store(QueryableStoreTypes.<String, Long>sessionStore(), 0);

    // Then:
    final InOrder inOrder = Mockito.inOrder(kafkaStreams);
    inOrder.verify(kafkaStreams).store(any());
  }

  @Test
  public void shouldRequestStore() {
    // Given:
    final QueryableStoreType<ReadOnlyWindowStore<Integer, Long>> storeType =
        QueryableStoreTypes.windowStore();

    // When:
    store.store(storeType, 0);

    // Then:
    verify(kafkaStreams).store(
        StoreQueryParameters.fromNameAndType(STORE_NAME, storeType).withPartition(0));
  }

  @Test
  public void shouldThrowIfStoreNotAvailableWhenRequested() {
    // Given:
    when(kafkaStreams.store(any())).thenThrow(new InvalidStateStoreException("boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> store.store(windowStore(), 0)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "State store currently unavailable: " + STORE_NAME));
    assertThat(e.getCause(), (instanceOf(InvalidStateStoreException.class)));
  }

  @Test
  public void shouldReturnSessionStore() {
    // Given:
    final ReadOnlySessionStore<?, ?> sessionStore = mock(ReadOnlySessionStore.class);
    when(kafkaStreams.store(any())).thenReturn(sessionStore);

    // When:
    final ReadOnlySessionStore<Double, String> result = store
        .store(QueryableStoreTypes.sessionStore(), 0);

    // Then:
    assertThat(result, is(sessionStore));
  }

  @Test
  public void shouldReturnWindowStore() {
    // Given:
    final ReadOnlyWindowStore<?, ?> windowStore = mock(ReadOnlyWindowStore.class);
    when(kafkaStreams.store(any())).thenReturn(windowStore);

    // When:
    final ReadOnlyWindowStore<Boolean, String> result = store
        .store(QueryableStoreTypes.windowStore(), 0);

    // Then:
    assertThat(result, is(windowStore));
  }
}