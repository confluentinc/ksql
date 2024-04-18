/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.execution.function.udaf;

import static io.confluent.ksql.GenericRow.genericRow;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.function.TableAggregationFunction;
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KudafUndoAggregatorTest {

  @Mock
  private TableAggregationFunction<Long, String, String> func1;
  @Mock
  private GenericKey key;
  private KudafUndoAggregator aggregator;

  @Before
  public void setUp() {
    aggregator = new KudafUndoAggregator(2, ImmutableList.of(func1));

    when(func1.undo(any(), any())).thenReturn("func1-undone");
  }

  @Test
  public void shouldNotMutateParametersOnApply() {
    // Given:
    final GenericRow value = GenericRow.genericRow(1, 2L);
    final GenericRow agg = GenericRow.genericRow(1, 2L, 3);

    // When:
    final GenericRow result = aggregator.apply(key, value, agg);

    // Then:
    assertThat(value, is(GenericRow.genericRow(1, 2L)));
    assertThat(agg, is(GenericRow.genericRow(1, 2L, 3)));
    assertThat("invalid test", result, is(not(GenericRow.genericRow(1, 2L, 3))));
  }

  @Test
  public void shouldApplyUndoableAggregateFunctions() {
    // Given:
    final GenericRow value = genericRow(1, 2L);
    final GenericRow aggRow = genericRow(1, 2L, 3);

    // When:
    final GenericRow resultRow = aggregator.apply(key, value, aggRow);

    // Then:
    assertThat(resultRow, equalTo(genericRow(1, 2L, "func1-undone")));
  }

  @Test
  public void shouldApplyUndoableMultiParamAggregateFunctions() {
    when(func1.convertToInput(any())).thenAnswer(
            (invocation) -> {
              List<?> inputs = invocation.getArgument(0, List.class);
              return Pair.of(inputs.get(0), inputs.get(1));
            }
    );
    when(func1.getArgIndicesInValue()).thenReturn(Arrays.asList(0, 1));

    // Given:
    final GenericRow value = genericRow(1, 2L);
    final GenericRow aggRow = genericRow(1, 2L, 3);

    // When:
    final GenericRow resultRow = aggregator.apply(key, value, aggRow);

    // Then:
    assertThat(resultRow, equalTo(genericRow(1, 2L, "func1-undone")));
  }

  @Test
  public void shouldApplyUndoableVariadicAggregateFunctions() {
    when(func1.convertToInput(any())).thenAnswer(
            (invocation) -> {
              List<?> inputs = invocation.getArgument(0, List.class);
              return Pair.of(inputs.get(0), new VariadicArgs<>(inputs.subList(1, 4)));
            }
    );
    when(func1.getArgIndicesInValue()).thenReturn(Arrays.asList(0, 1, 2, 3));

    // Given:
    final GenericRow value = genericRow(1, 2L, 3L, 4L);
    final GenericRow aggRow = genericRow(1, 2L, 3);

    // When:
    final GenericRow resultRow = aggregator.apply(key, value, aggRow);

    // Then:
    assertThat(resultRow, equalTo(genericRow(1, 2L, "func1-undone")));
  }
}