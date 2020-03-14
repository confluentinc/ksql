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

package io.confluent.ksql.function.udaf.topk;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class StringTopkKudafTest {
  private final List<String> valueArray = ImmutableList.of("10", "ab", "cde", "efg", "aa", "32", "why", "How are you",
      "Test", "123", "432");
  private TopKAggregateFunctionFactory topKFactory;
  private List<SqlType> argumentType;

  private final AggregateFunctionInitArguments args =
      new AggregateFunctionInitArguments(0, 3);

  @Before
  public void setup() {
    topKFactory = new TopKAggregateFunctionFactory();
    argumentType = Collections.singletonList(SqlTypes.STRING);
  }

  @Test
  public void shouldAggregateTopK() {
    final KsqlAggregateFunction<String, List<String>, List<String>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    List<String> currentVal = new ArrayList<>();
    for (final String value : valueArray) {
      currentVal = topkKudaf.aggregate(value , currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of("why", "efg", "cde")));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    final KsqlAggregateFunction<String, List<String>, List<String>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    List<String> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate("why", currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of("why")));
  }

  @Test
  public void shouldMergeTopK() {
    final KsqlAggregateFunction<String, List<String>, List<String>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    final List<String> array1 = ImmutableList.of("paper", "Hello", "123");
    final List<String> array2 = ImmutableList.of("Zzz", "Hi", "456");

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of("paper", "Zzz", "Hi")));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final KsqlAggregateFunction<String, List<String>, List<String>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    final List<String> array1 = ImmutableList.of("50", "45");
    final List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of("60", "50", "45")));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final KsqlAggregateFunction<String, List<String>, List<String>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    final List<String> array1 = ImmutableList.of("50");
    final List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of("60", "50")));
  }
}