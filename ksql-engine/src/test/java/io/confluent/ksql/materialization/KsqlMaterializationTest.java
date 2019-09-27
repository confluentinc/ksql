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

package io.confluent.ksql.materialization;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.util.StructKeyUtil;
import io.confluent.ksql.materialization.KsqlMaterialization.KsqlMaterializedTable;
import io.confluent.ksql.materialization.KsqlMaterialization.KsqlMaterializedWindowedTable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.SchemaUtil;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Predicate;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlMaterializationTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .keyColumn(SchemaUtil.ROWKEY_NAME, SqlTypes.STRING)
      .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
      .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
      .build();

  private static final Struct A_KEY = StructKeyUtil.asStructKey("k");
  private static final Range<Instant> WINDOW_START_BOUNDS = Range.closed(
      Instant.now(),
      Instant.now().plusSeconds(10)
  );
  private static final GenericRow A_VALUE = new GenericRow("a", "b");
  private static final GenericRow TRANSFORMED = new GenericRow("x", "y");
  private static final Window A_WINDOW = Window.of(Instant.now(), Optional.empty());

  private static final Row ROW = Row.of(
      SCHEMA,
      A_KEY,
      A_VALUE
  );

  private static final WindowedRow WINDOWED_ROW = WindowedRow.of(
      SCHEMA,
      A_KEY,
      A_WINDOW,
      A_VALUE
  );

  @Mock
  private Materialization inner;
  @Mock
  private Function<GenericRow, GenericRow> aggregateTransform;
  @Mock
  private Predicate<Struct, GenericRow> havingPredicate;
  @Mock
  private Function<GenericRow, GenericRow> storeToTableTransform;
  @Mock
  private MaterializedTable innerNonWindowed;
  @Mock
  private MaterializedWindowedTable innerWindowed;

  private KsqlMaterialization materialization;


  @Before
  public void setUp() {
    materialization = new KsqlMaterialization(
        inner,
        aggregateTransform,
        havingPredicate,
        storeToTableTransform,
        SCHEMA
    );

    when(inner.nonWindowed()).thenReturn(innerNonWindowed);
    when(inner.windowed()).thenReturn(innerWindowed);

    when(innerNonWindowed.get(any())).thenReturn(Optional.of(ROW));
    when(innerWindowed.get(any(), any())).thenReturn(ImmutableList.of(WINDOWED_ROW));

    when(aggregateTransform.apply(any())).thenAnswer(inv -> inv.getArgument(0));

    when(havingPredicate.test(any(), any())).thenReturn(true);

    when(storeToTableTransform.apply(any())).thenAnswer(inv -> inv.getArgument(0));
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(Materialization.class, inner)
        .setDefault(LogicalSchema.class, SCHEMA)
        .testConstructors(KsqlMaterialization.class, Visibility.PACKAGE);
  }

  @Test
  public void shouldReturnInnerLocator() {
    // Given:
    final Locator expected = mock(Locator.class);
    when(inner.locator()).thenReturn(expected);

    // When:
    final Locator locator = materialization.locator();

    // Then:
    assertThat(locator, is(sameInstance(expected)));
  }

  @Test
  public void shouldReturnInnerWindowType() {
    // Given:
    when(inner.windowType()).thenReturn(Optional.of(WindowType.SESSION));

    // When:
    final Optional<WindowType> windowType = materialization.windowType();

    // Then:
    assertThat(windowType, is(Optional.of(WindowType.SESSION)));
  }

  @Test
  public void shouldWrappedNonWindowed() {
    // When:
    final MaterializedTable table = materialization.nonWindowed();

    // Then:
    assertThat(table, is(instanceOf(KsqlMaterializedTable.class)));
  }

  @Test
  public void shouldWrappedWindowed() {
    // When:
    final MaterializedWindowedTable table = materialization.windowed();

    // Then:
    assertThat(table, is(instanceOf(KsqlMaterializedWindowedTable.class)));
  }

  @Test
  public void shouldCallInnerNonWindowedWithCorrectParamsOnGet() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();

    // When:
    table.get(A_KEY);

    // Then:
    verify(innerNonWindowed).get(A_KEY);
  }

  @Test
  public void shouldCallInnerWindowedWithCorrectParamsOnGet() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();

    // When:
    table.get(A_KEY, WINDOW_START_BOUNDS);

    // Then:
    verify(innerWindowed).get(A_KEY, WINDOW_START_BOUNDS);
  }

  @Test
  public void shouldCallHavingPredicateWithCorrectValuesOnNonWindowedGet() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();

    // When:
    table.get(A_KEY);

    // Then:
    verify(havingPredicate).test(A_KEY, A_VALUE);
  }

  @Test
  public void shouldCallHavingPredicateWithCorrectValuesOnWindowedGet() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();

    // When:
    table.get(A_KEY, WINDOW_START_BOUNDS);

    // Then:
    verify(havingPredicate).test(A_KEY, A_VALUE);
  }

  @Test
  public void shouldReturnEmptyIfInnerNonWindowedReturnsEmpty() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(innerNonWindowed.get(any())).thenReturn(Optional.empty());

    // When:
    final Optional<?> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyIfInnerWindowedReturnsEmpty() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    when(innerWindowed.get(any(), any())).thenReturn(ImmutableList.of());

    // When:
    final List<?> result = table.get(A_KEY, WINDOW_START_BOUNDS);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldFilterNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(havingPredicate.test(any(), any())).thenReturn(false);

    // When:
    final Optional<?> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldFilterWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    when(havingPredicate.test(any(), any())).thenReturn(false);

    // When:
    final List<?> result = table.get(A_KEY, WINDOW_START_BOUNDS);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldAggregateMapThenTransformRowThenFilterNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();

    // When:
    table.get(A_KEY);

    // Then:
    final InOrder inOrder = inOrder(aggregateTransform, havingPredicate, storeToTableTransform);
    inOrder.verify(aggregateTransform).apply(any());
    inOrder.verify(havingPredicate).test(any(), any());
    inOrder.verify(storeToTableTransform).apply(any());
  }

  @Test
  public void shouldAggregateMapThenTransformRowThenFilterWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();

    // When:
    table.get(A_KEY, WINDOW_START_BOUNDS);

    // Then:
    final InOrder inOrder = inOrder(aggregateTransform, havingPredicate, storeToTableTransform);
    inOrder.verify(aggregateTransform).apply(any());
    inOrder.verify(havingPredicate).test(any(), any());
    inOrder.verify(storeToTableTransform).apply(any());
  }

  @Test
  public void shouldUseAggregateTransformedFromNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(aggregateTransform.apply(any())).thenReturn(TRANSFORMED);

    // When:
    table.get(A_KEY);

    // Then:
    verify(havingPredicate).test(A_KEY, TRANSFORMED);
  }

  @Test
  public void shouldUseAggregateTransformedFromWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    when(aggregateTransform.apply(any())).thenReturn(TRANSFORMED);

    // When:
    table.get(A_KEY, AN_INSTANT, AN_INSTANT);

    // Then:
    verify(havingPredicate).test(A_KEY, TRANSFORMED);
  }

  @Test
  public void shouldCallTransformWithCorrectParamsNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();

    // When:
    table.get(A_KEY);

    // Then:
    verify(storeToTableTransform).apply(A_VALUE);
  }

  @Test
  public void shouldCallTransformWithCorrectParamsWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();

    // When:
    table.get(A_KEY, WINDOW_START_BOUNDS);

    // Then:
    verify(storeToTableTransform).apply(A_VALUE);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldReturnSelectTransformedFromNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(storeToTableTransform.apply(any())).thenReturn(TRANSFORMED);

    // When:
    final Optional<Row> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(not(Optional.empty())));
    assertThat(result.get().key(), is(A_KEY));
    assertThat(result.get().window(), is(Optional.empty()));
    assertThat(result.get().value(), is(TRANSFORMED));
  }

  @Test
  public void shouldReturnSelectTransformedFromWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    when(storeToTableTransform.apply(any())).thenReturn(TRANSFORMED);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, WINDOW_START_BOUNDS);

    // Then:
    assertThat(result, hasSize(1));
    assertThat(result.get(0).key(), is(A_KEY));
    assertThat(result.get(0).window(), is(Optional.of(A_WINDOW)));
    assertThat(result.get(0).value(), is(TRANSFORMED));
  }

  @Test
  public void shouldMaintainResultOrdering() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();

    final Window window1 = mock(Window.class);
    final Window window2 = mock(Window.class);
    final Window window3 = mock(Window.class);

    final ImmutableList<WindowedRow> rows = ImmutableList.of(
        WindowedRow.of(SCHEMA, A_KEY, window1, A_VALUE),
        WindowedRow.of(SCHEMA, A_KEY, window2, A_VALUE),
        WindowedRow.of(SCHEMA, A_KEY, window3, A_VALUE)
    );

    when(innerWindowed.get(any(), any())).thenReturn(rows);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, WINDOW_START_BOUNDS);

    // Then:
    assertThat(result, hasSize(rows.size()));
    assertThat(result.get(0).window(), is(Optional.of(window1)));
    assertThat(result.get(1).window(), is(Optional.of(window2)));
    assertThat(result.get(2).window(), is(Optional.of(window3)));
  }
}