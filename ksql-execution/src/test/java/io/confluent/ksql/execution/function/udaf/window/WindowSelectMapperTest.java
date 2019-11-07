/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.function.udaf.window;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.name.FunctionName;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.kafka.streams.kstream.Window;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionWindow;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class WindowSelectMapperTest {

  @Mock(MockType.NICE)
  private KsqlAggregateFunction<?, ?, ?> windowStartFunc;
  @Mock(MockType.NICE)
  private KsqlAggregateFunction<?, ?, ?> windowEndFunc;
  @Mock(MockType.NICE)
  private KsqlAggregateFunction<?, ?, ?> otherFunc;

  @Before
  public void setUp() {
    EasyMock.expect(windowStartFunc.getFunctionName()).andReturn(FunctionName.of("WinDowStarT")).anyTimes();
    EasyMock.expect(windowEndFunc.getFunctionName()).andReturn(FunctionName.of("WinDowEnD")).anyTimes();
    EasyMock.expect(otherFunc.getFunctionName()).andReturn(
        FunctionName.of("NotWindowStartOrWindowEnd")).anyTimes();
    EasyMock.replay(windowStartFunc, windowEndFunc, otherFunc);
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
    WindowSelectMapper mapper = new WindowSelectMapper(
        1,
        ImmutableList.of(otherFunc, windowStartFunc, windowEndFunc, windowStartFunc)
    );

    Window window = new SessionWindow(12345L, 54321L);
    GenericRow row = new GenericRow(Arrays.asList(0, 1, 2, 3, 4, 5));

    // When:
    GenericRow result = mapper.apply(new Windowed<>("k", window), row);

    // Then:
    assertThat(result, is(sameInstance(row)));
    assertThat(row.getColumns(), is(ImmutableList.of(0, 1, 12345L, 54321L, 12345L, 5)));
  }

  @Test(expected = IndexOutOfBoundsException.class)
  public void shouldThrowIfRowNotBigEnough() {
    // Given:
    WindowSelectMapper mapper = new WindowSelectMapper(0, ImmutableList.of(windowStartFunc));

    Window window = new SessionWindow(12345L, 54321L);
    GenericRow row = new GenericRow(new ArrayList<>());

    // When:
    mapper.apply(new Windowed<>("k", window), row);
  }
}