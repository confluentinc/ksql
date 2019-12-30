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

package io.confluent.ksql.execution.transform.window;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.FunctionName;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class WindowSelectMapperTest {

  @Mock
  private KsqlAggregateFunction<?, ?, ?> windowStartFunc;
  @Mock
  private KsqlAggregateFunction<?, ?, ?> windowEndFunc;
  @Mock
  private KsqlAggregateFunction<?, ?, ?> otherFunc;
  @Mock
  private KsqlProcessingContext ctx;

  @Before
  public void setUp() {
    when(windowStartFunc.name()).thenReturn(FunctionName.of("WinDowStarT"));
    when(windowEndFunc.name()).thenReturn(FunctionName.of("WinDowEnD"));
    when(otherFunc.name()).thenReturn(FunctionName.of("NotWindowStartOrWindowEnd"));
  }

  @Test
  public void shouldNotDetectNonWindowBoundsSelects() {
    assertThat(new WindowSelectMapper(5, ImmutableList.of(otherFunc)).hasSelects(),
        is(false));
  }

  @Test
  public void shouldDetectWindowStartSelects() {
    assertThat(new WindowSelectMapper(5, ImmutableList.of(windowStartFunc)).hasSelects(),
        is(true));
  }

  @Test
  public void shouldDetectWindowEndSelects() {
    assertThat(new WindowSelectMapper(5, ImmutableList.of(windowEndFunc)).hasSelects(),
        is(true));
  }

  @Test
  public void shouldUpdateRowWithWindowBounds() {
    // Given:
    final KsqlTransformer<Windowed<Object>, GenericRow> mapper = new WindowSelectMapper(
        1,
        ImmutableList.of(otherFunc, windowStartFunc, windowEndFunc, windowStartFunc)
    ).getTransformer();

    final Window window = new SessionWindow(12345L, 54321L);
    final GenericRow row = new GenericRow(Arrays.asList(0, 1, 2, 3, 4, 5));

    // When:
    final GenericRow result = mapper.transform(new Windowed<>("k", window), row, ctx);

    // Then:
    assertThat(result, is(sameInstance(row)));
    assertThat(row.getColumns(), is(ImmutableList.of(0, 1, 12345L, 54321L, 12345L, 5)));
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void shouldThrowIfRowNotBigEnough() {
    // Given:
    final KsqlTransformer<Windowed<Object>, GenericRow> mapper = new WindowSelectMapper(
        0,
        ImmutableList.of(windowStartFunc)
    ).getTransformer();

    final Window window = new SessionWindow(12345L, 54321L);
    final GenericRow row = new GenericRow(new ArrayList<>());

    // When:
    mapper.transform(new Windowed<>("k", window), row, ctx);
  }
}