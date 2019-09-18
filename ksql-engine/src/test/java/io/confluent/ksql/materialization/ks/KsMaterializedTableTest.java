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
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.materialization.MaterializationException;
import io.confluent.ksql.materialization.MaterializationTimeOutException;
import io.confluent.ksql.materialization.Row;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.structured.StructKeyUtil;
import java.util.Optional;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.state.QueryableStoreTypes.KeyValueStoreType;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsMaterializedTableTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn("ROWKEY", SqlTypes.STRING)
      .valueColumn("v0", SqlTypes.STRING)
      .build();

  private static final Struct A_KEY = StructKeyUtil.asStructKey("x");

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Mock
  private KsStateStore stateStore;
  @Mock
  private ReadOnlyKeyValueStore<Struct, GenericRow> tableStore;

  private KsMaterializedTable table;

  @Before
  public void setUp() {
    table = new KsMaterializedTable(stateStore);

    when(stateStore.store(any())).thenReturn(tableStore);
    when(stateStore.schema()).thenReturn(SCHEMA);
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(KsStateStore.class, stateStore)
        .testConstructors(KsMaterializedTable.class, Visibility.PACKAGE);
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
    table.get(A_KEY);
  }

  @Test
  public void shouldThrowIfStoreGetFails() {
    // Given:
    when(tableStore.get(any())).thenThrow(new MaterializationTimeOutException("Boom"));

    // Then:
    expectedException.expect(MaterializationException.class);
    expectedException.expectMessage("Failed to get value from materialized table");
    expectedException.expectCause(instanceOf(MaterializationTimeOutException.class));

    // When:
    table.get(A_KEY);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldGetStoreWithCorrectParams() {
    // When:
    table.get(A_KEY);

    // Then:
    verify(stateStore).store(any(KeyValueStoreType.class));
  }

  @Test
  public void shouldGetWithCorrectParams() {
    // When:
    table.get(A_KEY);

    // Then:
    verify(tableStore).get(A_KEY);
  }

  @Test
  public void shouldReturnEmptyIfKeyNotPresent() {
    // When:
    final Optional<?> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnValueIfKeyPresent() {
    // Given:
    final GenericRow value = new GenericRow("col0");
    when(tableStore.get(any())).thenReturn(value);

    // When:
    final Optional<Row> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(Optional.of(Row.of(SCHEMA, A_KEY, value))));
  }
}