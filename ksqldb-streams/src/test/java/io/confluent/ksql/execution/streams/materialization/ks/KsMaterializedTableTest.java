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

import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.MaterializationException;
import io.confluent.ksql.execution.streams.materialization.MaterializationTimeOutException;
import io.confluent.ksql.execution.streams.materialization.Row;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
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

  private static final Struct A_KEY = StructKeyUtil
      .keyBuilder(ColumnName.of("K0"), SqlTypes.STRING).build("x");
  private static final int PARTITION = 0;

  @Mock
  private KsStateStore stateStore;
  @Mock
  private ReadOnlyKeyValueStore<Struct, ValueAndTimestamp<GenericRow>> tableStore;
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
  public void shouldGetWithCorrectParams() {
    // When:
    table.get(A_KEY, PARTITION);

    // Then:
    verify(tableStore).get(A_KEY);
  }

  @Test
  public void shouldReturnEmptyIfKeyNotPresent() {
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
    when(tableStore.get(any())).thenReturn(ValueAndTimestamp.make(value, rowTime));

    // When:
    final Optional<Row> result = table.get(A_KEY, PARTITION);

    // Then:
    assertThat(result, is(Optional.of(Row.of(SCHEMA, A_KEY, value, rowTime))));
  }
}