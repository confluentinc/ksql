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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.util.Pair;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KudafAggregatorTest {

  @Mock
  private KsqlAggregateFunction<Long, String, String> func1;
  @Mock
  private GenericKey key;
  @Mock
  private Merger<GenericKey, String> func1Merger;
  @Mock
  private Function<String, String> func1ResultMapper;
  @Mock
  private KsqlProcessingContext ctx;
  private KudafAggregator<String> aggregator;

  @Before
  public void setUp()  {
    aggregator = new KudafAggregator<>(2, ImmutableList.of(func1));

    when(func1.getMerger()).thenReturn(func1Merger);
    when(func1.getResultMapper()).thenReturn(func1ResultMapper);

    when(func1.aggregate(any(), any())).thenReturn("func1-result");
    when(func1Merger.apply(any(), any(), any())).thenReturn("func1-merged");
    when(func1ResultMapper.apply(any())).thenReturn("func1-result");
  }

  @Test
  public void shouldNotMutateParametersOnApply() {
    // Given:
    final GenericRow value = GenericRow.genericRow(1, 2L);
    final GenericRow agg = GenericRow.genericRow(1, 2L, 3);

    // When:
    final GenericRow result = aggregator.apply("key", value, agg);

    // Then:
    assertThat(value, is(GenericRow.genericRow(1, 2L)));
    assertThat(agg, is(GenericRow.genericRow(1, 2L, 3)));
    assertThat("invalid test", result, is(not(GenericRow.genericRow(1, 2L, 3))));
  }

  @Test
  public void shouldNotMutateParametersOnMerge() {
    // Given:
    final GenericRow aggOne = GenericRow.genericRow(1, 2L, 4);
    final GenericRow aggTwo = GenericRow.genericRow(1, 2L, 3);

    // When:
    final GenericRow result = aggregator.getMerger().apply(key, aggOne, aggTwo);

    // Then:
    assertThat(aggOne, is(GenericRow.genericRow(1, 2L, 4)));
    assertThat(aggTwo, is(GenericRow.genericRow(1, 2L, 3)));
    assertThat("invalid test", result, is(not(GenericRow.genericRow(1, 2L, 4))));
    assertThat("invalid test", result, is(not(GenericRow.genericRow(1, 2L, 3))));
  }

  @Test
  public void shouldNotMutateParametersOnResultsMap() {
    // Given:
    final GenericRow agg = GenericRow.genericRow(1, 2L, 4);

    // When:
    final GenericRow result = aggregator.getResultMapper().transform("k", agg, ctx);

    // Then:
    assertThat(agg, is(GenericRow.genericRow(1, 2L, 4)));
    assertThat("invalid test", result, is(not(GenericRow.genericRow(1, 2L, 4))));
  }

  @Test
  public void shouldNotMutateParametersOnApplyMultiParam() {
    when(func1.convertToInput(any())).thenAnswer(
            (invocation) -> {
              List<?> inputs = invocation.getArgument(0, List.class);
              return Pair.of(inputs.get(0), inputs.get(1));
            }
    );
    when(func1.getArgIndicesInValue()).thenReturn(Arrays.asList(0, 1));

    // Given:
    final GenericRow value = GenericRow.genericRow(1, 2L);
    final GenericRow agg = GenericRow.genericRow(1, 2L, 3);

    // When:
    final GenericRow result = aggregator.apply("key", value, agg);

    // Then:
    assertThat(value, is(GenericRow.genericRow(1, 2L)));
    assertThat(agg, is(GenericRow.genericRow(1, 2L, 3)));
    assertThat("invalid test", result, is(not(GenericRow.genericRow(1, 2L, 3))));
  }

  @Test
  public void shouldNotMutateParametersOnApplyVariadicParam() {
    when(func1.convertToInput(any())).thenAnswer(
            (invocation) -> {
              List<?> inputs = invocation.getArgument(0, List.class);
              return Pair.of(inputs.get(0), new VariadicArgs<>(inputs.subList(1, 4)));
            }
    );
    when(func1.getArgIndicesInValue()).thenReturn(Arrays.asList(0, 1, 2, 3));

    // Given:
    final GenericRow value = GenericRow.genericRow(1, 2L, 3L, 4L);
    final GenericRow agg = GenericRow.genericRow(1, 2L, 3);

    // When:
    final GenericRow result = aggregator.apply("key", value, agg);

    // Then:
    assertThat(value, is(GenericRow.genericRow(1, 2L, 3L, 4L)));
    assertThat(agg, is(GenericRow.genericRow(1, 2L, 3)));
    assertThat("invalid test", result, is(not(GenericRow.genericRow(1, 2L, 3))));
  }
}