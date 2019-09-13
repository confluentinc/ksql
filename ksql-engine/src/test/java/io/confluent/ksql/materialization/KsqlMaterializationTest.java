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
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableMap;
import com.google.common.testing.NullPointerTester;
import com.google.common.testing.NullPointerTester.Visibility;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.materialization.KsqlMaterialization.KsqlMaterializedTable;
import io.confluent.ksql.materialization.KsqlMaterialization.KsqlMaterializedWindowedTable;
import io.confluent.ksql.model.WindowType;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.kafka.connect.data.SchemaBuilder;
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

  private static final Struct A_KEY = new Struct(SchemaBuilder.struct().build());
  private static final Instant AN_INSTANT = Instant.now();
  private static final Instant LATER_INSTANT = AN_INSTANT.plusSeconds(10);
  private static final GenericRow A_VALUE = new GenericRow("a", "b");
  private static final GenericRow TRANSFORMED = new GenericRow("x", "y");
  private static final Window A_WINDOW = Window.of(Instant.now(), Optional.empty());

  @Mock
  private Materialization inner;
  @Mock
  private Predicate<Struct, GenericRow> havingPredicate;
  @Mock
  private Function<GenericRow, GenericRow> storeToTableTransform;
  @Mock
  private MaterializedTable innerNonWindowed;
  @Mock
  private MaterializedWindowedTable innerWindowed;
  @Mock
  private LogicalSchema schema;
  private KsqlMaterialization materialization;


  @Before
  public void setUp() {
    materialization = new KsqlMaterialization(
        inner,
        havingPredicate,
        storeToTableTransform,
        schema
    );

    when(inner.nonWindowed()).thenReturn(innerNonWindowed);
    when(inner.windowed()).thenReturn(innerWindowed);

    when(innerNonWindowed.get(any())).thenReturn(Optional.of(A_VALUE));
    when(innerWindowed.get(any(), any(), any())).thenReturn(ImmutableMap.of(A_WINDOW, A_VALUE));

    when(havingPredicate.test(any(), any())).thenReturn(true);

    when(storeToTableTransform.apply(any())).thenAnswer(inv -> inv.getArgument(0));
  }

  @Test
  public void shouldThrowNPEs() {
    new NullPointerTester()
        .setDefault(Materialization.class, inner)
        .setDefault(LogicalSchema.class, schema)
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
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    verify(innerWindowed).get(A_KEY, AN_INSTANT, LATER_INSTANT);
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
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    verify(havingPredicate).test(A_KEY, A_VALUE);
  }

  @Test
  public void shouldReturnEmptyIfInnerNonWindowedReturnsEmpty() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(innerNonWindowed.get(any())).thenReturn(Optional.empty());

    // When:
    final Optional<GenericRow> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldReturnEmptyIfInnerWindowedReturnsEmpty() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    when(innerWindowed.get(any(), any(), any())).thenReturn(ImmutableMap.of());

    // When:
    final Map<Window, GenericRow> result = table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    assertThat(result.entrySet(), is(empty()));
  }

  @Test
  public void shouldFilterNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(havingPredicate.test(any(), any())).thenReturn(false);

    // When:
    final Optional<GenericRow> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(Optional.empty()));
  }

  @Test
  public void shouldFilterWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();
    when(havingPredicate.test(any(), any())).thenReturn(false);

    // When:
    final Map<Window, GenericRow> result = table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    assertThat(result.entrySet(), is(empty()));
  }

  @Test
  public void shouldTransformRowAfterFilterNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();

    // When:
    table.get(A_KEY);

    // Then:
    final InOrder inOrder = inOrder(havingPredicate, storeToTableTransform);
    inOrder.verify(havingPredicate).test(any(), any());
    inOrder.verify(storeToTableTransform).apply(any());
  }

  @Test
  public void shouldTransformRowAfterFilterWindowed() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();

    // When:
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    final InOrder inOrder = inOrder(havingPredicate, storeToTableTransform);
    inOrder.verify(havingPredicate).test(any(), any());
    inOrder.verify(storeToTableTransform).apply(any());
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
    table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    verify(storeToTableTransform).apply(A_VALUE);
  }

  @Test
  public void shouldReturnTransformedFromNonWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(storeToTableTransform.apply(any())).thenReturn(TRANSFORMED);

    // When:
    final Optional<GenericRow> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(Optional.of(TRANSFORMED)));
  }

  @Test
  public void shouldReturnTransformedFromWindowed() {
    // Given:
    final MaterializedTable table = materialization.nonWindowed();
    when(storeToTableTransform.apply(any())).thenReturn(TRANSFORMED);

    // When:
    final Optional<GenericRow> result = table.get(A_KEY);

    // Then:
    assertThat(result, is(Optional.of(TRANSFORMED)));
  }

  @Test
  public void shouldMaintainResultOrdering() {
    // Given:
    final MaterializedWindowedTable table = materialization.windowed();

    final Window window1 = mock(Window.class);
    final Window window2 = mock(Window.class);
    final Window window3 = mock(Window.class);
    when(innerWindowed.get(any(), any(), any())).thenReturn(ImmutableMap.of(
        window1, new GenericRow(),
        window2, new GenericRow(),
        window3, new GenericRow()
    ));

    // When:
    final Map<Window, GenericRow> result = table.get(A_KEY, AN_INSTANT, LATER_INSTANT);

    // Then:
    assertThat(result.keySet(), contains(window1, window2, window3));
  }
}