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
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

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
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.query.KeyQuery;
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

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializedTableTestIQv2 {

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

  @Mock
  private KafkaStreams kafkaStreams;
  @Mock
  private KsStateStore stateStore;
  @Mock
  private KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>> keyValueIterator;
  @Mock
  private StateQueryResult queryResult;
  @Mock
  private QueryResult<KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>>> rangeResult;
  @Mock
  private QueryResult<ValueAndTimestamp<GenericRow>> keyResult;
  @Mock
  private ValueAndTimestamp<GenericRow> row;
  @Captor
  private ArgumentCaptor<StateQueryRequest<?>> queryTypeCaptor;

  private KsMaterializedTable table;

  @Before
  public void setUp() {
    table = new KsMaterializedTable(stateStore);

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
    assertThat(e.getMessage(), containsString(
        "Failed to get value from materialized table"));
    assertThat(e.getCause(), (instanceOf(MaterializationTimeOutException.class)));
  }

  @Test
  public void shouldKeyQueryWithCorrectParams() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(keyResult);
    when(keyResult.getResult()).thenReturn(row);
    when(row.value()).thenReturn(ROW1);

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
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);

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
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);

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
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);

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
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);

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
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);

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
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(keyResult);

    // When:
    final Optional<?> result = table.get(A_KEY, PARTITION);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnValueIfKeyPresent() {
    // Given:
    final GenericRow value = GenericRow.genericRow("col0");
    final long rowTime = 2343553L;
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(keyResult);
    when(keyResult.getResult()).thenReturn(ValueAndTimestamp.make(value, rowTime));

    // When:
    final Optional<Row> result = table.get(A_KEY, PARTITION);

    // Then:
    assertThat(result, is(Optional.of(Row.of(SCHEMA, A_KEY, value, rowTime))));
  }

  @Test
  public void shouldReturnValuesFullTableScan() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(true, true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE1)
        .thenReturn(KEY_VALUE2);

    // When:
    Iterator<Row> rows = table.get(PARTITION);

    // Then:
    assertThat(rows.next(), is(Row.of(SCHEMA, A_KEY, ROW1, TIME1)));
    assertThat(rows.next(), is(Row.of(SCHEMA, A_KEY2, ROW2, TIME2)));
    assertThat(rows.hasNext(), is(false));
  }

  @Test
  public void shouldReturnValuesLowerBound() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE2);

    // When:
    Iterator<Row> rows = table.get(PARTITION, A_KEY2, null);

    // Then:
    assertThat(rows.next(), is(Row.of(SCHEMA, A_KEY2, ROW2, TIME2)));
    assertThat(rows.hasNext(), is(false));
  }

  @Test
  public void shouldReturnValuesUpperBound() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE1);

    // When:
    Iterator<Row> rows = table.get(PARTITION, null, A_KEY);

    // Then:
    assertThat(rows.next(), is(Row.of(SCHEMA, A_KEY, ROW1, TIME1)));
    assertThat(rows.hasNext(), is(false));
  }

  @Test
  public void shouldReturnValuesBothBound() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(true, true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE1)
        .thenReturn(KEY_VALUE2);

    // When:
    Iterator<Row> rows = table.get(PARTITION, A_KEY, A_KEY2);

    // Then:
    assertThat(rows.next(), is(Row.of(SCHEMA, A_KEY, ROW1, TIME1)));
    assertThat(rows.next(), is(Row.of(SCHEMA, A_KEY2, ROW2, TIME2)));
    assertThat(rows.hasNext(), is(false));
  }


  @Test
  public void shouldCloseIterator_fullTableScan() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(true, true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE1)
        .thenReturn(KEY_VALUE2);

    // When:
    Streams.stream(table.get(PARTITION))
        .collect(Collectors.toList());

    // Then:
    verify(keyValueIterator).close();
  }

  @Test
  public void shouldCloseIterator_rangeBothBounds() {
    // Given:
    when(kafkaStreams.query(any())).thenReturn(queryResult);
    when(queryResult.getOnlyPartitionResult()).thenReturn(rangeResult);
    when(rangeResult.getResult()).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(true, true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE1)
        .thenReturn(KEY_VALUE2);

    // When:
    Streams.stream(table.get(PARTITION, A_KEY, A_KEY2))
        .collect(Collectors.toList());

    // Then:
    verify(keyValueIterator).close();
  }
}