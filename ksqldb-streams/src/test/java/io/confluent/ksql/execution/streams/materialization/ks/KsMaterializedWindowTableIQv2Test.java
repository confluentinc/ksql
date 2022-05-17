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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Range;
import com.google.common.collect.Streams;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializationTimeOutException;
import io.confluent.ksql.execution.streams.materialization.WindowedRow;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.query.WindowKeyQuery;
import org.apache.kafka.streams.query.WindowRangeQuery;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializedWindowTableIQv2Test {

  private static final Duration WINDOW_SIZE = Duration.ofMinutes(1);
  private static final int PARTITION = 0;

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .build();

  private static final GenericKey A_KEY = GenericKey.genericKey(0);
  private static final GenericKey A_KEY2 = GenericKey.genericKey(1);
  private static final GenericKey A_KEY3 = GenericKey.genericKey(1);

  protected static final Instant NOW = Instant.ofEpochMilli(System.currentTimeMillis());

  private static final Range<Instant> WINDOW_START_BOUNDS = Range.closed(
      NOW,
      NOW.plusSeconds(10)
  );

  private static final Range<Instant> WINDOW_END_BOUNDS = Range.closed(
      NOW.plusSeconds(5).plus(WINDOW_SIZE),
      NOW.plusSeconds(15).plus(WINDOW_SIZE)
  );

  private static final ValueAndTimestamp<GenericRow> VALUE_1 = ValueAndTimestamp
      .make(GenericRow.genericRow("col0"), 12345L);
  private static final ValueAndTimestamp<GenericRow> VALUE_2 = ValueAndTimestamp
      .make(GenericRow.genericRow("col1"), 45678L);
  private static final ValueAndTimestamp<GenericRow> VALUE_3 = ValueAndTimestamp
      .make(GenericRow.genericRow("col2"), 987865L);
  private static final String TOPIC = "topic";
  private static final long OFFSET = 100L;
  private static final Position POSITION = Position.fromMap(
      ImmutableMap.of(TOPIC, ImmutableMap.of(PARTITION, OFFSET)));

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KsStateStore stateStore;
  @Mock
  private WindowStoreIterator<ValueAndTimestamp<GenericRow>> fetchIterator;
  @Mock
  private KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>> keyValueIterator;

  private KsMaterializedWindowTableIQv2 table;
  @Captor
  private ArgumentCaptor<StateQueryRequest<?>> queryTypeCaptor;

  @Before
  public void setUp() {
    table = new KsMaterializedWindowTableIQv2(stateStore, WINDOW_SIZE);

    when(stateStore.getStateStoreName()).thenReturn("rocksdb-store");
    when(stateStore.getKafkaStreams()).thenReturn(kafkaStreams);
    when(stateStore.schema()).thenReturn(SCHEMA);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsStateStore.class, stateStore)
        .testConstructors(KsMaterializedWindowTable.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowQueryThrows() {
    // Given:
    when(kafkaStreams.query(any())).thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Boom"));
    assertThat(e, (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldThrowIfQueryFails() {
    // Given:
    final StateQueryResult<?> partitionResult = new StateQueryResult<>();
    partitionResult.addResult(PARTITION, QueryResult.forFailure(FailureReason.STORE_EXCEPTION, "Boom"));
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    // When:
    final Exception e = assertThrows(
      MaterializationException.class,
      () -> table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
      "Failed to get value from materialized table"));
    assertThat(e, (instanceOf(MaterializationException.class)));
  }

  @Test
  public void shouldThrowIfQueryThrows_fetchAll() {
    // Given:
    when(kafkaStreams.query(any())).thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Boom"));
    assertThat(e, (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldThrowIfQueryFails_fethAll() {
    // Given:
    final StateQueryResult<?> partitionResult = new StateQueryResult<>();
    partitionResult.addResult(PARTITION, QueryResult.forFailure(FailureReason.STORE_EXCEPTION, "Boom"));
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Boom"));
    assertThat(e, (instanceOf(MaterializationException.class)));
  }

  @Test
  public void shouldThrowIfStoreFetchFails_fetchAll() {
    // Given:
    when(kafkaStreams.query(any()))
      .thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
    );

    // Then:
    assertThat(e.getMessage(), containsString(
        "Boom"));
    assertThat(e, (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnValuesForClosedStartBounds() {
    // Given:
    final Range<Instant> start = Range.closed(
      Instant.ofEpochMilli(System.currentTimeMillis()),
      NOW.plusSeconds(10)
    );

    final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> result = QueryResult.forResult(fetchIterator);
    result.setPosition(POSITION);
    partitionResult.addResult(PARTITION, result);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);
    when(fetchIterator.hasNext()).thenReturn(true, true, false);

    when(fetchIterator.next())
      .thenReturn(new KeyValue<>(start.lowerEndpoint().toEpochMilli(), VALUE_1))
      .thenReturn(new KeyValue<>(start.upperEndpoint().toEpochMilli(), VALUE_2))
      .thenThrow(new AssertionError());

    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(A_KEY, PARTITION, start, Range.all()).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
      WindowedRow.of(
        SCHEMA,
        windowedKey(start.lowerEndpoint()),
        VALUE_1.value(),
        VALUE_1.timestamp()
      ),
      WindowedRow.of(
        SCHEMA,
        windowedKey(start.upperEndpoint()),
        VALUE_2.value(),
        VALUE_2.timestamp()
      )
    ));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnValuesForClosedStartBounds_fetchAll() {
    // Given:
    final Range<Instant> start = Range.closed(
      Instant.ofEpochMilli(System.currentTimeMillis()),
      NOW.plusSeconds(10)
    );

    final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);
    when(keyValueIterator.hasNext()).thenReturn(true, true, false);

    when(keyValueIterator.next())
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY,
        new TimeWindow(start.lowerEndpoint().toEpochMilli(),
          start.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_1))
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY2,
        new TimeWindow(start.upperEndpoint().toEpochMilli(),
          start.upperEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_2))
      .thenThrow(new AssertionError());


    // When:
    final KsMaterializedQueryResult<WindowedRow> result =
        table.get(PARTITION, start, Range.all());

    // Then:
    final Iterator<WindowedRow> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
      is (WindowedRow.of(
        SCHEMA,
        windowedKey(A_KEY, start.lowerEndpoint()),
        VALUE_1.value(),
        VALUE_1.timestamp())));
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
      is(WindowedRow.of(
        SCHEMA,
        windowedKey(A_KEY2, start.upperEndpoint()),
        VALUE_2.value(),
        VALUE_2.timestamp())));
    assertThat(rowIterator.hasNext(), is(false));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCloseIterator() {
    // When:
    final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> result = QueryResult.forResult(fetchIterator);
    result.setPosition(POSITION);
    partitionResult.addResult(PARTITION, result);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);
    when(fetchIterator.hasNext()).thenReturn(false);

    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(fetchIterator).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldCloseIterator_fetchAll() {
    // When:
    final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);
    when(keyValueIterator.hasNext()).thenReturn(false);

    Streams.stream((table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS)
            .getRowIterator()))
        .collect(Collectors.toList());

    // Then:
    verify(keyValueIterator).close();
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnEmptyIfKeyNotPresent() {
    // When:
    final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> result = QueryResult.forResult(fetchIterator);
    result.setPosition(POSITION);
    partitionResult.addResult(PARTITION, result);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);
    when(fetchIterator.hasNext()).thenReturn(false);

    final Iterator<WindowedRow> rowIterator = table.get(
        A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(false));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnEmptyIfKeyNotPresent_fetchAll() {
    // When:
    final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);
    when(keyValueIterator.hasNext()).thenReturn(false);

    final Iterator<WindowedRow> rowIterator = table.get(
        PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(false));
  }


  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnValuesForClosedEndBounds() {
    // Given:
    final Range<Instant> end = Range.closed(
      NOW,
      NOW.plusSeconds(10)
    );

    final Range<Instant> startEqiv = Range.closed(
      end.lowerEndpoint().minus(WINDOW_SIZE),
      end.lowerEndpoint().minus(WINDOW_SIZE)
    );

    final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> partitionResult =
        new StateQueryResult<>();
    final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(fetchIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    when(fetchIterator.hasNext())
      .thenReturn(true)
      .thenReturn(true)
      .thenReturn(false);

    when(fetchIterator.next())
      .thenReturn(new KeyValue<>(startEqiv.lowerEndpoint().toEpochMilli(), VALUE_1))
      .thenReturn(new KeyValue<>(startEqiv.upperEndpoint().toEpochMilli(), VALUE_2))
      .thenThrow(new AssertionError());

    // When:
    final KsMaterializedQueryResult<WindowedRow> result = table.get(
        A_KEY, PARTITION, Range.all(), end);

    // Then:
    final Iterator<WindowedRow> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
      WindowedRow.of(
        SCHEMA,
        windowedKey(startEqiv.lowerEndpoint()),
        VALUE_1.value(),
        VALUE_1.timestamp()
      ),
      WindowedRow.of(
        SCHEMA,
        windowedKey(startEqiv.upperEndpoint()),
        VALUE_2.value(),
        VALUE_2.timestamp()
      )
    ));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnValuesForClosedEndBounds_fetchAll() {
    // Given:
    final Range<Instant> end = Range.closed(
      NOW,
      NOW.plusSeconds(10)
    );

    final Range<Instant> startEqiv = Range.closed(
      end.lowerEndpoint().minus(WINDOW_SIZE),
      end.lowerEndpoint().minus(WINDOW_SIZE)
    );

    final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    when(keyValueIterator.hasNext())
      .thenReturn(true, true, false);

    when(keyValueIterator.next())
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY,
        new TimeWindow(startEqiv.lowerEndpoint().toEpochMilli(),
          startEqiv.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_1))
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY2,
        new TimeWindow(startEqiv.upperEndpoint().toEpochMilli(),
          startEqiv.upperEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_2))
      .thenThrow(new AssertionError());


    // When:
    final KsMaterializedQueryResult<WindowedRow> result =
        table.get(PARTITION, Range.all(), end);

    // Then:
    final Iterator<WindowedRow> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
      is (WindowedRow.of(
        SCHEMA,
        windowedKey(startEqiv.lowerEndpoint()),
        VALUE_1.value(),
        VALUE_1.timestamp())));
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
      is(WindowedRow.of(
        SCHEMA,
        windowedKey(A_KEY2, startEqiv.upperEndpoint()),
        VALUE_2.value(),
        VALUE_2.timestamp())));
    assertThat(rowIterator.hasNext(), is(false));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnValuesForOpenStartBounds() {
    // Given:
    final Range<Instant> start = Range.open(
      NOW,
      NOW.plusSeconds(10)
    );

    final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> partitionResult =
        new StateQueryResult<>();
    final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(fetchIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    when(fetchIterator.hasNext())
      .thenReturn(true)
      .thenReturn(true)
      .thenReturn(true)
      .thenReturn(false);

    when(fetchIterator.next())
      .thenReturn(new KeyValue<>(start.lowerEndpoint().toEpochMilli(), VALUE_1))
      .thenReturn(new KeyValue<>(start.lowerEndpoint().plusMillis(1).toEpochMilli(), VALUE_2))
      .thenReturn(new KeyValue<>(start.upperEndpoint().toEpochMilli(), VALUE_3))
      .thenThrow(new AssertionError());

    // When:
    final KsMaterializedQueryResult<WindowedRow> result = table.get(
        A_KEY, PARTITION, start, Range.all());

    // Then:
    final Iterator<WindowedRow> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(), is(
        WindowedRow.of(
            SCHEMA,
          windowedKey(start.lowerEndpoint().plusMillis(1)),
          VALUE_2.value(),
          VALUE_2.timestamp()
        )
    ));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnValuesForOpenStartBounds_fetchAll() {
    // Given:
    final Range<Instant> start = Range.open(
      NOW,
      NOW.plusSeconds(10)
    );

    final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    when(keyValueIterator.hasNext())
      .thenReturn(true, true, true, false);

    when(keyValueIterator.next())
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY,
        new TimeWindow(start.lowerEndpoint().toEpochMilli(),
          start.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_1))
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY2,
        new TimeWindow(start.lowerEndpoint().plusMillis(1).toEpochMilli(),
          start.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis() + 1)), VALUE_2))
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY3,
        new TimeWindow(start.upperEndpoint().toEpochMilli(),
          start.upperEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_3))
      .thenThrow(new AssertionError());


    // When:
    final KsMaterializedQueryResult<WindowedRow> result =
        table.get(PARTITION, start, Range.all());

    // Then:
    final Iterator<WindowedRow> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
      is (WindowedRow.of(
        SCHEMA,
        windowedKey(A_KEY2, start.lowerEndpoint().plusMillis(1)),
        VALUE_2.value(),
        VALUE_2.timestamp())));
    assertThat(rowIterator.hasNext(), is(false));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnValuesForOpenEndBounds() {
    // Given:
    final Range<Instant> end = Range.open(
      NOW,
      NOW.plusSeconds(10)
    );

    final Range<Instant> startEquiv = Range.open(
      end.lowerEndpoint().minus(WINDOW_SIZE),
      end.upperEndpoint().minus(WINDOW_SIZE)
    );

    final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> partitionResult =
        new StateQueryResult<>();
    final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(fetchIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    when(fetchIterator.hasNext())
      .thenReturn(true)
      .thenReturn(true)
      .thenReturn(true)
      .thenReturn(false);

    when(fetchIterator.next())
      .thenReturn(new KeyValue<>(startEquiv.lowerEndpoint().toEpochMilli(), VALUE_1))
      .thenReturn(
        new KeyValue<>(startEquiv.lowerEndpoint().plusMillis(1).toEpochMilli(), VALUE_2))
      .thenReturn(new KeyValue<>(startEquiv.upperEndpoint().toEpochMilli(), VALUE_3))
      .thenThrow(new AssertionError());

    // When:
    final KsMaterializedQueryResult<WindowedRow> result =
        table.get(A_KEY, PARTITION, Range.all(), end);

    // Then:
    final Iterator<WindowedRow> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
      WindowedRow.of(
        SCHEMA,
        windowedKey(startEquiv.lowerEndpoint().plusMillis(1)),
        VALUE_2.value(),
        VALUE_2.timestamp()
      )
    ));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldReturnValuesForOpenEndBounds_fetchAll() {
    // Given:
    final Range<Instant> end = Range.open(
      NOW,
      NOW.plusSeconds(10)
    );

    final Range<Instant> startEquiv = Range.open(
      end.lowerEndpoint().minus(WINDOW_SIZE),
      end.upperEndpoint().minus(WINDOW_SIZE)
    );

    final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    when(keyValueIterator.hasNext())
      .thenReturn(true, true, true, false);

    when(keyValueIterator.next())
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY,
        new TimeWindow(startEquiv.lowerEndpoint().toEpochMilli(),
          startEquiv.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_1))
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY2,
        new TimeWindow(startEquiv.lowerEndpoint().plusMillis(1).toEpochMilli(),
          startEquiv.lowerEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis() + 1)), VALUE_2))
      .thenReturn(new KeyValue<>(new Windowed<>(A_KEY3,
        new TimeWindow(startEquiv.upperEndpoint().toEpochMilli(),
          startEquiv.upperEndpoint().toEpochMilli() + WINDOW_SIZE.toMillis())), VALUE_3))
      .thenThrow(new AssertionError());


    // When:
    final KsMaterializedQueryResult<WindowedRow> result =
        table.get(PARTITION, Range.all(), end);

    // Then:
    final Iterator<WindowedRow> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(),
      is (WindowedRow.of(
        SCHEMA,
        windowedKey(A_KEY2, startEquiv.lowerEndpoint().plusMillis(1)),
        VALUE_2.value(),
        VALUE_2.timestamp())));
    assertThat(rowIterator.hasNext(), is(false));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldMaintainResultOrder() {
    // Given:
    when(fetchIterator.hasNext())
      .thenReturn(true)
      .thenReturn(true)
      .thenReturn(true)
      .thenReturn(false);

    final Instant start = WINDOW_START_BOUNDS.lowerEndpoint();

    final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> partitionResult =
        new StateQueryResult<>();
    final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> result = QueryResult.forResult(fetchIterator);
    result.setPosition(POSITION);
    partitionResult.addResult(PARTITION, result);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    when(fetchIterator.next())
      .thenReturn(new KeyValue<>(start.toEpochMilli(), VALUE_1))
      .thenReturn(new KeyValue<>(start.plusMillis(1).toEpochMilli(), VALUE_2))
      .thenReturn(new KeyValue<>(start.plusMillis(2).toEpochMilli(), VALUE_3))
      .thenThrow(new AssertionError());

    // When:
    final Iterator<WindowedRow> rowIterator =
        table.get(A_KEY, PARTITION, Range.all(), Range.all()).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(true));
    final List<WindowedRow> resultList = Lists.newArrayList(rowIterator);
    assertThat(resultList, contains(
      WindowedRow.of(
        SCHEMA,
        windowedKey(start),
        VALUE_1.value(),
        VALUE_1.timestamp()
      ),
      WindowedRow.of(
        SCHEMA,
        windowedKey(start.plusMillis(1)),
        VALUE_2.value(),
        VALUE_2.timestamp()
      ),
      WindowedRow.of(
        SCHEMA,
        windowedKey(start.plusMillis(2)),
        VALUE_3.value(),
        VALUE_3.timestamp()
      )
    ));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSupportRangeAll() {
    // When:
    final StateQueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> partitionResult =
        new StateQueryResult<>();
    final QueryResult<WindowStoreIterator<ValueAndTimestamp<GenericRow>>> result = QueryResult.forResult(fetchIterator);
    result.setPosition(POSITION);
    partitionResult.addResult(PARTITION, result);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    table.get(A_KEY, PARTITION, Range.all(), Range.all());

    // Then:
    verify(kafkaStreams).query(queryTypeCaptor.capture());
    StateQueryRequest<?> request = queryTypeCaptor.getValue();
    assertThat(request.getQuery(), instanceOf(WindowKeyQuery.class));
    WindowKeyQuery<GenericKey,GenericRow> keyQuery = (WindowKeyQuery<GenericKey,GenericRow>) request.getQuery();
    assertThat(keyQuery.getKey(), is(A_KEY));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void shouldSupportRangeAll_fetchAll() {
    // When:
    final StateQueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> partitionResult = new StateQueryResult<>();
    final QueryResult<KeyValueIterator<Windowed<GenericKey>, ValueAndTimestamp<GenericRow>>> queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    partitionResult.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any(StateQueryRequest.class))).thenReturn(partitionResult);

    table.get(PARTITION, Range.all(), Range.all());

    // Then:
    verify(kafkaStreams).query(queryTypeCaptor.capture());
    StateQueryRequest<?> request = queryTypeCaptor.getValue();
    assertThat(request.getQuery(), instanceOf(WindowRangeQuery.class));
  }

  private static Windowed<GenericKey> windowedKey(final Instant windowStart) {
    return windowedKey(A_KEY, windowStart);
  }

  private static Windowed<GenericKey> windowedKey(GenericKey key, final Instant windowStart) {
    return new Windowed<>(
      key,
      new TimeWindow(windowStart.toEpochMilli(), windowStart.plus(WINDOW_SIZE).toEpochMilli())
    );
  }
}