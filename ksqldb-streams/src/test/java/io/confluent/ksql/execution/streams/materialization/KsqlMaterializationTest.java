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

package io.confluent.ksql.execution.streams.materialization;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Range;
import com.google.common.collect.Streams;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterialization.KsqlMaterializedTable;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterialization.KsqlMaterializedWindowedTable;
import io.confluent.ksql.execution.streams.materialization.KsqlMaterialization.Transform;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import org.mockito.InOrder;
import org.mockito.Mock;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KsqlMaterializationTest {

  private static LogicalSchema SCHEMA;
  private static GenericKey A_KEY;
  private static GenericKey A_KEY2;
  private static long A_ROWTIME;
  private static Range<Instant> WINDOW_START_BOUNDS;
  private static Range<Instant> WINDOW_END_BOUNDS;
  private static int PARTITION;
  private static GenericRow A_VALUE;
  private static GenericRow A_VALUE2;
  private static GenericRow TRANSFORMED;
  private static Window A_WINDOW;
  private static TimeWindow STREAM_WINDOW;
  private static Row ROW;
  private static Row ROW2;
  private static WindowedRow WINDOWED_ROW;
  private static WindowedRow WINDOWED_ROW2;


  @Mock
  private Materialization inner;
  @Mock
  private Transform project;
  @Mock
  private Transform filter;
  @Mock
  private MaterializedTable innerNonWindowed;
  @Mock
  private MaterializedWindowedTable innerWindowed;

  private KsqlMaterialization materialization;

  @Before
  public void setUp() {
    SCHEMA = LogicalSchema.builder()
            .keyColumn(ColumnName.of("k0"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("v0"), SqlTypes.STRING)
            .valueColumn(ColumnName.of("v1"), SqlTypes.STRING)
            .build();

    A_KEY = GenericKey.genericKey("k");
    A_KEY2 = GenericKey.genericKey("k2");

    A_ROWTIME = 12335L;

    WINDOW_START_BOUNDS = Range.closed(
            Instant.now(),
            Instant.now().plusSeconds(10)
    );

    WINDOW_END_BOUNDS = Range.closed(
            Instant.now().plusSeconds(1),
            Instant.now().plusSeconds(11)
    );
    PARTITION = 0;

    A_VALUE = GenericRow.genericRow("a", "b");
    A_VALUE2 = GenericRow.genericRow("a2", "b2");
    TRANSFORMED = GenericRow.genericRow("x", "y");
    A_WINDOW = Window.of(Instant.now(), Instant.now().plusMillis(10));
    STREAM_WINDOW = new TimeWindow(
            A_WINDOW.start().toEpochMilli(),
            A_WINDOW.end().toEpochMilli()
    );

    ROW = Row.of(
            SCHEMA,
            A_KEY,
            A_VALUE,
            A_ROWTIME
    );

    ROW2 = Row.of(
            SCHEMA,
            A_KEY2,
            A_VALUE2,
            A_ROWTIME
    );

    WINDOWED_ROW = WindowedRow.of(
            SCHEMA,
            new Windowed<>(A_KEY, STREAM_WINDOW),
            A_VALUE,
            A_ROWTIME
    );

    WINDOWED_ROW2 = WindowedRow.of(
            SCHEMA,
            new Windowed<>(A_KEY2, STREAM_WINDOW),
            A_VALUE2,
            A_ROWTIME
    );

    materialization = new KsqlMaterialization(
        inner,
        SCHEMA,
        ImmutableList.of(filter, project)
    );

    when(inner.nonWindowed()).thenReturn(innerNonWindowed);
    when(inner.windowed()).thenReturn(innerWindowed);

    when(innerNonWindowed.get(any(), anyInt())).thenReturn(Optional.of(ROW));
    when(innerNonWindowed.get(anyInt())).thenReturn(Iterators.forArray(ROW, ROW2));
    when(innerWindowed.get(any(), anyInt(), any(), any())).thenReturn(ImmutableList.of(WINDOWED_ROW));
    when(innerWindowed.get(anyInt(), any(), any()))
        .thenReturn(ImmutableList.of(WINDOWED_ROW, WINDOWED_ROW2).iterator());
  }

  @SuppressWarnings("UnstableApiUsage")
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
    givenNoopTransforms();

    // When:
    final Locator locator = materialization.locator();

    // Then:
    assertThat(locator, is(sameInstance(expected)));
  }

  @Test
  public void shouldReturnInnerWindowType() {
    // Given:
    when(inner.windowType()).thenReturn(Optional.of(WindowType.SESSION));
    givenNoopTransforms();

    // When:
    final Optional<WindowType> windowType = materialization.windowType();

    // Then:
    assertThat(windowType, is(Optional.of(WindowType.SESSION)));
  }

  @Test
  public void shouldWrappedNonWindowed() {
    // Given:
    givenNoopTransforms();

    // When:
    final MaterializedTable table = materialization.nonWindowed();

    // Then:
    assertThat(table, is(instanceOf(KsqlMaterializedTable.class)));
  }

  @Test
  public void shouldWrappedWindowed() {
    // Given:
    givenNoopTransforms();

    // When:
    final MaterializedWindowedTable table = materialization.windowed();

    // Then:
    assertThat(table, is(instanceOf(KsqlMaterializedWindowedTable.class)));
  }

  @Test
  public void shouldCallInnerNonWindowedWithCorrectParamsOnGet() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    table.get(A_KEY, PARTITION);

    // Then:
    verify(innerNonWindowed).get(A_KEY, PARTITION);
  }

  @Test
  public void shouldCallInnerWindowedWithCorrectParamsOnGet() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(innerWindowed).get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);
  }

  @Test
  public void shouldCallFilterWithCorrectValuesOnNonWindowedGet() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));


    // When:
    table.get(A_KEY, PARTITION);

    // Then:
    verify(filter).apply(A_KEY, A_VALUE, new PullProcessingContext(A_ROWTIME));
  }

  @Test
  public void shouldCallFilterWithCorrectValuesOnWindowedGet() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(filter).apply(
        new Windowed<>(A_KEY, STREAM_WINDOW),
        A_VALUE,
        new PullProcessingContext(A_ROWTIME)
    );
  }

  @Test
  public void shouldReturnEmptyIfInnerNonWindowedReturnsEmpty() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(innerNonWindowed.get(any(), anyInt())).thenReturn(Optional.empty());
    givenNoopTransforms();

    // When:
    final Optional<?> result = table.get(A_KEY, PARTITION);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyIfInnerWindowedReturnsEmpty() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    when(innerWindowed.get(any(), anyInt(), any(), any())).thenReturn(ImmutableList.of());
    givenNoopTransforms();

    // When:
    final List<?> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldFilterNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    givenNoopProject();
    when(filter.apply(any(), any(), any())).thenReturn(Optional.empty());

    // When:
    final Optional<?> result = table.get(A_KEY, PARTITION);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldFilterWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    givenNoopProject();
    when(filter.apply(any(), any(), any())).thenReturn(Optional.empty());

    // When:
    final List<?> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    assertThat(result, is(empty()));
  }

  @Test
  public void shouldFilterNonWindowed_fullScan() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    givenNoopProject();
    when(filter.apply(any(), any(), any())).thenReturn(Optional.empty());

    // When:
    final Iterator<?> result = table.get(PARTITION);

    // Then:
    assertThat(result.hasNext(), is(false));
  }

  @Test
  public void shouldFilterWindowed_fullScan() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    givenNoopProject();
    when(filter.apply(any(), any(), any())).thenReturn(Optional.empty());

    // When:
    final Iterator<?> result = table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    assertThat(result.hasNext(), is(false));
  }

  @Test
  public void shouldCallTransformsInOrder() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    table.get(A_KEY, PARTITION);

    // Then:
    final InOrder inOrder = inOrder(project, filter);
    inOrder.verify(filter).apply(any(), any(), any());
    inOrder.verify(project).apply(any(), any(), any());
  }

  @Test
  public void shouldCallTransformsInOrderForWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    final InOrder inOrder = inOrder(project, filter);
    inOrder.verify(filter).apply(any(), any(), any());
    inOrder.verify(project).apply(any(), any(), any());
  }

  @Test
  public void shouldPipeTransforms() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    givenNoopProject();
    when(filter.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    table.get(A_KEY, PARTITION);

    // Then:
    verify(project).apply(A_KEY, TRANSFORMED, new PullProcessingContext(A_ROWTIME));
  }

  @Test
  public void shouldPipeTransformsWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    givenNoopProject();
    when(filter.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS);

    // Then:
    verify(project).apply(
        new Windowed<>(A_KEY, STREAM_WINDOW),
        TRANSFORMED,
        new PullProcessingContext(A_ROWTIME)
    );
  }

  @Test
  public void shouldPipeTransforms_fullTableScan() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    givenNoopProject();
    when(filter.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    Streams.stream(table.get(PARTITION)).collect(Collectors.toList());


    // Then:
    verify(project).apply(A_KEY, TRANSFORMED, new PullProcessingContext(A_ROWTIME));
    verify(project).apply(A_KEY2, TRANSFORMED, new PullProcessingContext(A_ROWTIME));
  }

  @Test
  public void shouldPipeTransformsWindowed_fullTableScan() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    givenNoopProject();
    when(filter.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    Streams.stream(table.get(PARTITION, WINDOW_START_BOUNDS, WINDOW_END_BOUNDS))
        .collect(Collectors.toList());

    // Then:
    verify(project).apply(
        new Windowed<>(A_KEY, STREAM_WINDOW),
        TRANSFORMED,
        new PullProcessingContext(A_ROWTIME)
    );
    verify(project).apply(
        new Windowed<>(A_KEY2, STREAM_WINDOW),
        TRANSFORMED,
        new PullProcessingContext(A_ROWTIME)
    );
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  @Test
  public void shouldReturnSelectTransformedFromNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    final Optional<Row> result = table.get(A_KEY, PARTITION);

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
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS,
        WINDOW_END_BOUNDS);

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
    givenNoopFilter();
    when(project.apply(any(), any(), any())).thenReturn(Optional.of(TRANSFORMED));

    final Instant now = Instant.now();
    final TimeWindow window1 =
        new TimeWindow(now.plusMillis(10).toEpochMilli(), now.plusMillis(11).toEpochMilli());

    final SessionWindow window2 =
        new SessionWindow(now.toEpochMilli(), now.plusMillis(2).toEpochMilli());

    final TimeWindow window3 =
        new TimeWindow(now.toEpochMilli(), now.plusMillis(3).toEpochMilli());

    final ImmutableList<WindowedRow> rows = ImmutableList.of(
        WindowedRow.of(SCHEMA, new Windowed<>(A_KEY, window1), A_VALUE, A_ROWTIME),
        WindowedRow.of(SCHEMA, new Windowed<>(A_KEY, window2), A_VALUE, A_ROWTIME),
        WindowedRow.of(SCHEMA, new Windowed<>(A_KEY, window3), A_VALUE, A_ROWTIME)
    );

    when(innerWindowed.get(any(), anyInt(), any(), any())).thenReturn(rows);

    // When:
    final List<WindowedRow> result = table.get(A_KEY, PARTITION, WINDOW_START_BOUNDS,
        WINDOW_END_BOUNDS);

    // Then:
    assertThat(result, hasSize(rows.size()));
    assertThat(result.get(0).windowedKey().window(), is(window1));
    assertThat(result.get(1).windowedKey().window(), is(window2));
    assertThat(result.get(2).windowedKey().window(), is(window3));
  }

  @Test
  public void shouldReturnIntermediateRowNonWindowed() {
    // Given:
    final GenericRow intermediateRow1 = A_VALUE.append(A_ROWTIME).append(A_KEY);
    final GenericRow intermediateRow2 = A_VALUE2.append(A_ROWTIME).append(A_KEY2);

    // When:
    final GenericRow genericRow1 = KsqlMaterialization.getIntermediateRow(ROW);
    final GenericRow genericRow2 = KsqlMaterialization.getIntermediateRow(ROW2);

    // Then:
    assertThat(genericRow1, is(intermediateRow1));
    assertThat(genericRow2, is(intermediateRow2));
  }

  @Test
  public void shouldReturnIntermediateRowWindowed() {
    // Given:
    final GenericRow intermediateRow1 = A_VALUE
            .append(A_ROWTIME)
            .append(A_KEY)
            .append(A_WINDOW.start().toEpochMilli())
            .append(A_WINDOW.end().toEpochMilli());

    final GenericRow intermediateRow2 = A_VALUE2
            .append(A_ROWTIME)
            .append(A_KEY2)
            .append(A_WINDOW.start().toEpochMilli())
            .append(A_WINDOW.end().toEpochMilli());

    // When:
    final GenericRow genericRow1 = KsqlMaterialization.getIntermediateRow(WINDOWED_ROW);
    final GenericRow genericRow2 = KsqlMaterialization.getIntermediateRow(WINDOWED_ROW2);

    // Then:
    assertThat(genericRow1, is(intermediateRow1));
    assertThat(genericRow2, is(intermediateRow2));
  }

  private void givenNoopFilter() {
    when(filter.apply(any(), any(), any()))
        .thenAnswer(inv -> Optional.of(inv.getArgument(1)));
  }

  private void givenNoopProject() {
    when(project.apply(any(), any(), any()))
        .thenAnswer(inv -> Optional.of(inv.getArgument(1)));
  }

  private void givenNoopTransforms() {
    givenNoopFilter();
    givenNoopProject();
  }
}