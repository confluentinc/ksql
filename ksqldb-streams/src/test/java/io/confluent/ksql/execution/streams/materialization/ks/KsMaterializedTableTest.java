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
import static org.mockito.ArgumentMatchers.anyInt;
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
import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Collectors;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializedTableTest {

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

  @Mock
  private KsStateStore stateStore;
  @Mock
  private ReadOnlyKeyValueStore<GenericKey, ValueAndTimestamp<GenericRow>> tableStore;
  @Mock
  private KeyValueIterator<GenericKey, ValueAndTimestamp<GenericRow>> keyValueIterator;
  @Captor
  private ArgumentCaptor<QueryableStoreType<?>> storeTypeCaptor;

  private KsMaterializedTable table;

  @Before
  public void setUp() {
    table = new KsMaterializedTable(stateStore);

    when(stateStore.store(any(), anyInt())).thenReturn(tableStore);
    when(stateStore.schema()).thenReturn(SCHEMA);
  }

  @SuppressWarnings("UnstableApiUsage")
  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsStateStore.class, stateStore)
        .testConstructors(KsMaterializedTable.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowIfGettingStateStoreFails() {
    // Given:
    when(stateStore.store(any(), anyInt())).thenThrow(new MaterializationTimeOutException("Boom"));

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
  public void shouldThrowIfStoreGetFails() {
    // Given:
    when(tableStore.get(any())).thenThrow(new MaterializationTimeOutException("Boom"));

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
  public void shouldGetStoreWithCorrectParams() {
    // When:
    table.get(A_KEY, PARTITION);

    // Then:
    verify(stateStore).store(storeTypeCaptor.capture(), anyInt());
    assertThat(storeTypeCaptor.getValue().getClass().getSimpleName(), is("TimestampedKeyValueStoreType"));
  }

  @Test
  public void shouldGetStoreWithCorrectParams_fullTableScan() {
    // Given:
    when(tableStore.all()).thenReturn(keyValueIterator);

    // When:
    table.get(PARTITION);

    // Then:
    verify(stateStore).store(storeTypeCaptor.capture(), anyInt());
    assertThat(storeTypeCaptor.getValue().getClass().getSimpleName(), is("TimestampedKeyValueStoreType"));
  }

  @Test
  public void shouldGetWithCorrectParams() {
    // When:
    table.get(A_KEY, PARTITION);

    // Then:
    verify(tableStore).get(A_KEY);
  }

  @Test
  public void shouldReturnEmptyIfKeyNotPresent() {
    // When:
    final Iterator<Row> result = table.get(A_KEY, PARTITION).rowIterator;

    // Then:
    assertThat(result, is(Collections.emptyIterator()));
  }

  @Test
  public void shouldReturnValueIfKeyPresent() {
    // Given:
    final GenericRow value = GenericRow.genericRow("col0");
    final long rowTime = 2343553L;
    when(tableStore.get(any())).thenReturn(ValueAndTimestamp.make(value, rowTime));

    // When:
    final Iterator<Row> rowIterator = table.get(A_KEY, PARTITION).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY, value, rowTime)));
  }

  @Test
  public void shouldReturnValuesFullTableScan() {
    // Given:
    when(tableStore.all()).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(true, true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE1)
        .thenReturn(KEY_VALUE2);

    // When:
    final Iterator<Row> rowIterator = table.get(PARTITION).rowIterator;

    // Then:
    assertThat(rowIterator.hasNext(), is(true));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY, ROW1, TIME1)));
    assertThat(rowIterator.next(), is(Row.of(SCHEMA, A_KEY2, ROW2, TIME2)));
    assertThat(rowIterator.hasNext(), is(false));
  }

  @Test
  public void shouldCloseIterator_fullTableScan() {
    // Given:
    when(tableStore.all()).thenReturn(keyValueIterator);
    when(keyValueIterator.hasNext()).thenReturn(true, true, false);
    when(keyValueIterator.next())
        .thenReturn(KEY_VALUE1)
        .thenReturn(KEY_VALUE2);

    // When:
    Streams.stream(table.get(PARTITION).rowIterator)
        .collect(Collectors.toList());

    // Then:
    verify(keyValueIterator).close();
  }
}