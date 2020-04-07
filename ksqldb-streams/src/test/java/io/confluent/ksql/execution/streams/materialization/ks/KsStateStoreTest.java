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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
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
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlySessionStore;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_NO_SIDE_EFFECT")
@RunWith(MockitoJUnitRunner.class)
public class KsStateStoreTest {

  private static final String STORE_NAME = "someStore";
  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
      .keyColumn(ColumnName.of("v0"), SqlTypes.BIGINT)
      .build();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KsqlConfig ksqlConfig;

  private KsStateStore store;

  @Before
  public void setUp() {
    store = new KsStateStore(STORE_NAME, kafkaStreams, SCHEMA, ksqlConfig);
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
  public void shouldNotAwaitRunning() {
    // Given:
    final QueryableStoreType<ReadOnlySessionStore<String, Long>> storeType =
        QueryableStoreTypes.sessionStore();

    // When:
    store.store(storeType);

    // Then:
    verify(kafkaStreams, never()).state();
  }

  @Test
  public void shouldThrowIfNotRunningAfterFailedToGetStore() {
    // Given:
    when(kafkaStreams.state())
        .thenReturn(State.RUNNING)
        .thenReturn(State.NOT_RUNNING);
    when(kafkaStreams.store(any())).thenThrow(new IllegalStateException());

    // When:
    expectedException.expect(MaterializationException.class);
    expectedException.expectMessage("State store currently unavailable: someStore");

    // When:
    store.store(QueryableStoreTypes.sessionStore());
  }

  @Test
  public void shouldGetStoreOnceRunning() {
    // When:
    store.store(QueryableStoreTypes.<String, Long>sessionStore());

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
    store.store(storeType);

    // Then:
    verify(kafkaStreams).store(StoreQueryParameters.fromNameAndType(STORE_NAME, storeType));
  }

  @Test
  public void shouldThrowIfStoreNotAvailableWhenRequested() {
    // Given:
    when(kafkaStreams.store(any())).thenThrow(new InvalidStateStoreException("boom"));

    // Then:
    expectedException.expect(MaterializationException.class);
    expectedException.expectMessage("State store currently unavailable: " + STORE_NAME);
    expectedException.expectCause(instanceOf(InvalidStateStoreException.class));

    // When:
    store.store(QueryableStoreTypes.windowStore());
  }

  @Test
  public void shouldReturnSessionStore() {
    // Given:
    final ReadOnlySessionStore<?, ?> sessionStore = mock(ReadOnlySessionStore.class);
    when(kafkaStreams.store(any())).thenReturn(sessionStore);

    // When:
    final ReadOnlySessionStore<Double, String> result = store
        .store(QueryableStoreTypes.sessionStore());

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
        .store(QueryableStoreTypes.windowStore());

    // Then:
    assertThat(result, is(windowStore));
  }
}