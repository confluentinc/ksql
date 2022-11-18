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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializationTimeOutException;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.FailureReason;
import org.apache.kafka.streams.query.KeyQuery;
import org.apache.kafka.streams.query.Position;
import org.apache.kafka.streams.query.QueryResult;
import org.apache.kafka.streams.query.RangeQuery;
import org.apache.kafka.streams.query.StateQueryRequest;
import org.apache.kafka.streams.query.StateQueryResult;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@SuppressWarnings("unchecked")
@RunWith(MockitoJUnitRunner.class)
public class KsMaterializedTableIQv2Test {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(ColumnName.of("K0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .build();

  private static final GenericKey A_KEY = GenericKey.genericKey("x");
  private static final GenericKey A_KEY2 = GenericKey.genericKey("y");
  private static final int PARTITION = 0;
  private static final GenericRow ROW1 = GenericRow.genericRow("col0");
  private static final GenericRow ROW2 = GenericRow.genericRow("col1");
  private static final long TIME1 = 1;
  private static final long TIME2 = 2;
  private static final ValueAndTimestamp<GenericRow> VALUE_AND_TIMESTAMP1
      = ValueAndTimestamp.make(ROW1, TIME1);
  private static final ValueAndTimestamp<GenericRow> VALUE_AND_TIMESTAMP2
      = ValueAndTimestamp.make(ROW2, TIME2);
  private static final KeyValue<GenericKey, ValueAndTimestamp<GenericRow>> KEY_VALUE1
      = KeyValue.pair(A_KEY, VALUE_AND_TIMESTAMP1);
  private static final KeyValue<GenericKey, ValueAndTimestamp<GenericRow>> KEY_VALUE2
      = KeyValue.pair(A_KEY2, VALUE_AND_TIMESTAMP2);
  private static final String STATE_STORE_NAME = "store";
  private static final String TOPIC = "topic";
  private static final long OFFSET = 100L;
  private static final Position POSITION = Position.fromMap(
      ImmutableMap.of(TOPIC, ImmutableMap.of(PARTITION, OFFSET)));

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KsStateStore stateStore;
  @Mock
  private KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>> keyValueIterator;
  @Captor
  private ArgumentCaptor<StateQueryRequest<?>> queryTypeCaptor;

  private KsMaterializedTableIQv2 table;

  @Before
  public void setUp() {
    table = new KsMaterializedTableIQv2(stateStore);

    when(stateStore.schema()).thenReturn(SCHEMA);
    when(stateStore.getStateStoreName()).thenReturn(STATE_STORE_NAME);
    when(stateStore.getKafkaStreams()).thenReturn(kafkaStreams);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsStateStore.class, stateStore)
        .testConstructors(KsMaterializedTable.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowIfQueryFails() {
    // Given:
    when(stateStore.getKafkaStreams().query(any())).thenThrow(new MaterializationTimeOutException("Boom"));

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(A_KEY, PARTITION)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Boom"));
    assertThat(e, (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  public void shouldThrowIfKeyQueryResultIsError() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getErrorResult());

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(A_KEY, PARTITION)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Error!"));
    assertThat(e, (instanceOf(MaterializationException.class)));
  }

  @Test
  public void shouldThrowIfRangeQueryResultIsError() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getErrorResult());

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(PARTITION, A_KEY, A_KEY2)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Error!"));
    assertThat(e, (instanceOf(MaterializationException.class)));
  }

  @Test
  public void shouldThrowIfTableScanQueryResultIsError() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getErrorResult());

    // When:
    final Exception e = assertThrows(
        MaterializationException.class,
        () -> table.get(PARTITION)
    );

    // Then:
    assertThat(e.getMessage(), containsString("Error!"));
    assertThat(e, (instanceOf(MaterializationException.class)));
  }


  @Test
  public void shouldKeyQueryWithCorrectParams() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getRowResult(ROW1));

    // When:
    table.get(A_KEY, PARTITION);

    // Then:
    verify(kafkaStreams).query(queryTypeCaptor.capture());
    StateQueryRequest request = queryTypeCaptor.getValue();
    assertThat(request.getQuery(), instanceOf(KeyQuery.class));
    KeyQuery keyQuery = (KeyQuery)request.getQuery();
    assertThat(keyQuery.getKey(), is(A_KEY));
  }

  @Test
  public void shouldRangeQueryWithCorrectParams_fullTableScan() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    table.get(PARTITION);

    // Then:
    verify(kafkaStreams).query(queryTypeCaptor.capture());
    StateQueryRequest request = queryTypeCaptor.getValue();
    assertThat(request.getQuery(), instanceOf(RangeQuery.class));
    RangeQuery rangeQuery = (RangeQuery)request.getQuery();
    assertThat(rangeQuery.getLowerBound(), is(Optional.empty()));
    assertThat(rangeQuery.getUpperBound(), is(Optional.empty()));
  }

  @Test
  public void shouldRangeQueryWithCorrectParams_lowerBound() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    table.get(PARTITION, A_KEY, null);

    // Then:
    verify(kafkaStreams).query(queryTypeCaptor.capture());
    StateQueryRequest request = queryTypeCaptor.getValue();
    assertThat(request.getQuery(), instanceOf(RangeQuery.class));
    RangeQuery rangeQuery = (RangeQuery)request.getQuery();
    assertThat(rangeQuery.getLowerBound(), is(Optional.of(A_KEY)));
    assertThat(rangeQuery.getUpperBound(), is(Optional.empty()));
  }

  @Test
  public void shouldRangeQueryWithCorrectParams_upperBound() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    table.get(PARTITION, null, A_KEY2);

    // Then:
    verify(kafkaStreams).query(queryTypeCaptor.capture());
    StateQueryRequest request = queryTypeCaptor.getValue();
    assertThat(request.getQuery(), instanceOf(RangeQuery.class));
    RangeQuery rangeQuery = (RangeQuery)request.getQuery();
    assertThat(rangeQuery.getLowerBound(), is(Optional.empty()));
    assertThat(rangeQuery.getUpperBound(), is(Optional.of(A_KEY2)));
  }

  @Test
  public void shouldRangeQueryWithCorrectParams_bothBounds() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    table.get(PARTITION, A_KEY, A_KEY2);

    // Then:
    verify(kafkaStreams).query(queryTypeCaptor.capture());
    StateQueryRequest request = queryTypeCaptor.getValue();
    assertThat(request.getQuery(), instanceOf(RangeQuery.class));
    RangeQuery rangeQuery = (RangeQuery)request.getQuery();
    assertThat(rangeQuery.getLowerBound(), is(Optional.of(A_KEY)));
    assertThat(rangeQuery.getUpperBound(), is(Optional.of(A_KEY2)));
  }

  @Test
  public void shouldRangeQueryWithCorrectParams_noBounds() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    table.get(PARTITION, null, null);

    // Then:
    verify(kafkaStreams).query(queryTypeCaptor.capture());
    StateQueryRequest request = queryTypeCaptor.getValue();
    assertThat(request.getQuery(), instanceOf(RangeQuery.class));
    RangeQuery rangeQuery = (RangeQuery)request.getQuery();
    assertThat(rangeQuery.getLowerBound(), is(Optional.empty()));
    assertThat(rangeQuery.getUpperBound(), is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyIfKeyNotPresent() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getRowResult(null));

    // When:
    final Iterator<Row> result = table.get(A_KEY, PARTITION).rowIterator;

    // Then:
    assertThat(result, is(Collections.emptyIterator()));
  }

  @Test
  public void shouldReturnEmptyIfRangeNotPresent() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getEmptyIteratorResult());

    // When:
    final Iterator<Row> rowIterator = table.get(PARTITION, A_KEY, null).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(false));
  }


  @Test
  public void shouldReturnValueIfKeyPresent() {
    // Given:
    final GenericRow value = GenericRow.genericRow("col0");
    final long rowTime = -1L;
    when(kafkaStreams.query(any())).thenReturn(getRowResult(ROW1));

    // When:
    final Iterator<Row> rowIterator = table.get(A_KEY, PARTITION).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY, value, rowTime)));
  }

  @Test
  public void shouldReturnValuesFullTableScan() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    final KsMaterializedQueryResult<Row> result = table.get(PARTITION);

    // Then:
    Iterator<Row> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY, ROW1, TIME1)));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY2, ROW2, TIME2)));
    assertThat(rowIterator.hasNext(), is(false));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  public void shouldReturnValuesLowerBound() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    final KsMaterializedQueryResult<Row> result = table.get(PARTITION, A_KEY, null);

    // Then:
    Iterator<Row> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY, ROW1, TIME1)));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY2, ROW2, TIME2)));
    assertThat(rowIterator.hasNext(), is(false));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }


  @Test
  public void shouldReturnValuesUpperBound() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    final KsMaterializedQueryResult<Row> result = table.get(PARTITION, null, A_KEY2);

    // Then:
    Iterator<Row> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY, ROW1, TIME1)));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY2, ROW2, TIME2)));
    assertThat(rowIterator.hasNext(), is(false));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }

  @Test
  public void shouldReturnValuesBothBound() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(getIteratorResult());

    // When:
    final KsMaterializedQueryResult<Row> result = table.get(PARTITION, A_KEY, A_KEY2);

    // Then:
    Iterator<Row> rowIterator = result.getRowIterator();
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY, ROW1, TIME1)));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY2, ROW2, TIME2)));
    assertThat(rowIterator.hasNext(), is(false));
    assertThat(result.getPosition(), not(Optional.empty()));
    assertThat(result.getPosition().get(), is(POSITION));
  }


  @Test
  public void shouldCloseIterator_fullTableScan() {
    // Given:
    final StateQueryResult result = new StateQueryResult();
    final QueryResult queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    result.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any())).thenReturn(result);

    // When:
    Streams.stream((table.get(PARTITION).getRowIterator()))
        .collect(Collectors.toList());

    // Then:
    verify(keyValueIterator).close();
  }

  @Test
  public void shouldCloseIterator_rangeBothBounds() {
    // Given:
    final StateQueryResult result = new StateQueryResult();
    final QueryResult queryResult = QueryResult.forResult(keyValueIterator);
    queryResult.setPosition(POSITION);
    result.addResult(PARTITION, queryResult);
    when(kafkaStreams.query(any())).thenReturn(result);
    when(keyValueIterator.hasNext()).thenReturn(true, true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE1)
        .thenReturn(KEY_VALUE2);

    // When:
    Streams.stream(table.get(PARTITION, A_KEY, A_KEY2).rowIterator)
        .collect(Collectors.toList());

    // Then:
    verify(keyValueIterator).close();
  }

  private static StateQueryResult getRowResult(final GenericRow row) {
    final StateQueryResult result = new StateQueryResult<>();
    final QueryResult queryResult = QueryResult.forResult(ValueAndTimestamp.make(row, -1));
    final Position position = Position.emptyPosition();
    position.withComponent(TOPIC, PARTITION, OFFSET);
    queryResult.setPosition(position);
    result.addResult(PARTITION, queryResult);
    return result;
  }

  private static StateQueryResult getErrorResult() {
    final StateQueryResult result = new StateQueryResult<>();
    result.addResult(PARTITION, QueryResult.forFailure(FailureReason.NOT_ACTIVE, "Error!"));
    return result;
  }

  private static StateQueryResult getIteratorResult() {
    final StateQueryResult result = new StateQueryResult<>();
    Set<GenericKey> keySet = new HashSet<>();
    keySet.add(A_KEY);
    keySet.add(A_KEY2);
    Map<GenericKey, ValueAndTimestamp<GenericRow>> map = new HashMap<>();
    map.put(A_KEY, VALUE_AND_TIMESTAMP1);
    map.put(A_KEY2, VALUE_AND_TIMESTAMP2);
    final KeyValueIterator iterator = new TestKeyValueIterator(keySet, map);
    final QueryResult queryResult = QueryResult.forResult(iterator);
    queryResult.setPosition(POSITION);
    result.addResult(PARTITION, queryResult);
    return result;
  }

  private static StateQueryResult getEmptyIteratorResult() {
    final StateQueryResult result = new StateQueryResult<>();
    Set<GenericKey> keySet = new HashSet<>();
    Map<GenericKey, ValueAndTimestamp<GenericRow>> map = new HashMap<>();
    final KeyValueIterator iterator = new TestKeyValueIterator(keySet, map);
    final QueryResult queryResult = QueryResult.forResult(iterator);
    queryResult.setPosition(POSITION);
    result.addResult(PARTITION, queryResult);
    return result;
  }

  private static Position getTestPosition() {
    final Position position = Position.emptyPosition();
    position.withComponent(TOPIC, PARTITION, OFFSET);
    Position.fromMap(ImmutableMap.of(TOPIC, ImmutableMap.of(PARTITION, OFFSET)));
    return position;
  }

  private static class TestKeyValueIterator implements
      KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>>
  {
    private final Iterator<GenericKey> iter;
    private final Map<GenericKey, ValueAndTimestamp<GenericRow>> map;

    private TestKeyValueIterator(final Set<GenericKey> keySet, final Map<GenericKey,
        ValueAndTimestamp<GenericRow>> map) {
        this.iter = keySet.iterator();
        this.map = map;
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public KeyValue<GenericKey, ValueAndTimestamp<GenericRow>> next() {
      final GenericKey key = iter.next();
      return new KeyValue<>(key, map.get(key));
    }

    @Override
    public void close() {
      // do nothing
    }

    @Override
    public GenericKey peekNextKey() {
      throw new UnsupportedOperationException("peekNextKey() not supported in " + getClass().getName());
    }
  }
}