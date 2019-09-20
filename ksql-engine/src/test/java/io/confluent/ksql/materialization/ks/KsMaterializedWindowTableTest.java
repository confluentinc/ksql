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

package io.confluent.ksql.materialization.ks;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.materialization.MaterializationException;
import io.confluent.ksql.materialization.MaterializationTimeOutException;
import io.confluent.ksql.materialization.Window;
import io.confluent.ksql.materialization.WindowedRow;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.QueryableStoreTypes.WindowStoreType;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializedWindowTableTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn("ROWKEY", SqlTypes.STRING)
      .valueColumn("v0", SqlTypes.STRING)
      .build();

  private static final Struct A_KEY = StructKeyUtil.asStructKey("x");
  private static final Instant AN_INSTANT = Instant.now();
  private static final Instant LATER_INSTANT = AN_INSTANT.plusSeconds(10);

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsStateStore stateStore;
  @Mock
  private ReadOnlyWindowStore<Struct, GenericRow> tableStore;
  @Mock
  private WindowStoreIterator<GenericRow> fetchIterator;

  private KsMaterializedWindowTable table;

  @Before
  public void setUp() {
    table = new KsMaterializedWindowTable(stateStore);

    when(stateStore.store(any())).thenReturn(tableStore);
    when(stateStore.schema()).thenReturn(SCHEMA);
    when(tableStore.fetch(any(), any(), any())).thenReturn(fetchIterator);
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsStateStore.class, stateStore)
        .testConstructors(KsMaterializedWindowTable.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldThrowIfGettingStateStoreFails() {
    // Given:
    when(stateStore.store(any())).thenThrow(new MaterializationTimeOutException("Boom"));

    // Then:
    expectedException.expect(MaterializationException.class);
    expectedException.expectMessage("Failed to get value from materialized table");
    expectedException.expectCause(instanceOf(MaterializationTimeOutException.class));

    // When:
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);
  }

  @Test
  public void shouldThrowIfStoreFetchFails() {
    // Given:
    when(tableStore.fetch(any(), any(), any()))
        .thenThrow(new MaterializationTimeOutException("Boom"));

    // Then:
    expectedException.expect(MaterializationException.class);
    expectedException.expectMessage("Failed to get value from materialized table");
    expectedException.expectCause(instanceOf(MaterializationTimeOutException.class));

    // When:
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGetStoreWithCorrectParams() {
    // When:
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    verify(stateStore).store(any(WindowStoreType.class));
  }

  @Test
  public void shouldFetchWithCorrectParams() {
    // When:
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    verify(tableStore).fetch(A_KEY, AN_INSTANT, LATER_INSTANT);
  }

  @Test
  public void shouldCloseIterator() {
    // When:
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    verify(fetchIterator).close();
  }

  @Test
  public void shouldReturnEmptyIfKeyNotPresent() {
    // When:
    final List<?> result = table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldReturnValueIfKeyPresent() {
    // Given:
    final GenericRow value1 = new GenericRow("col0");
    final GenericRow value2 = new GenericRow("col1");

    when(fetchIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(fetchIterator.next())
        .thenReturn(new KeyValue<>(1L, value1))
        .thenReturn(new KeyValue<>(2L, value2))
        .thenThrow(new AssertionError());

    when(tableStore.fetch(any(), any(), any())).thenReturn(fetchIterator);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    assertThat(result, contains(
        WindowedRow.of(SCHEMA, A_KEY, Window.of(Instant.ofEpochMilli(1), Optional.empty()), value1),
        WindowedRow.of(SCHEMA, A_KEY, Window.of(Instant.ofEpochMilli(2), Optional.empty()), value2)
    ));
  }

  @Test
  public void shouldMaintainResultOrder() {
    // Given:
    when(fetchIterator.hasNext())
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(true)
        .thenReturn(false);

    when(fetchIterator.next())
        .thenReturn(new KeyValue<>(1L, new GenericRow("a")))
        .thenReturn(new KeyValue<>(3L, new GenericRow("b")))
        .thenReturn(new KeyValue<>(2L, new GenericRow("c")))
        .thenThrow(new AssertionError());

    when(tableStore.fetch(any(), any(), any())).thenReturn(fetchIterator);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    assertThat(result, contains(
        WindowedRow.of(
            SCHEMA,
            A_KEY,
            Window.of(Instant.ofEpochMilli(1), Optional.empty()),
            new GenericRow("a")
        ),
        WindowedRow.of(
            SCHEMA,
            A_KEY,
            Window.of(Instant.ofEpochMilli(3), Optional.empty()),
            new GenericRow("b")
        ),
        WindowedRow.of(
            SCHEMA,
            A_KEY,
            Window.of(Instant.ofEpochMilli(2), Optional.empty()),
            new GenericRow("c")
        )
    ));
  }
}