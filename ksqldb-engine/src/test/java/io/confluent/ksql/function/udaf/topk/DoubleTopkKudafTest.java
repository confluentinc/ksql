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
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class DoubleTopkKudafTest {
  private final List<Double> valuesArray = ImmutableList.of(10.0, 30.0, 45.0, 10.0, 50.0, 60.0, 20.0, 60.0, 80.0, 35.0,
      25.0);
  private TopKAggregateFunctionFactory topKFactory;
  private List<SqlArgument> argumentType;

  private final AggregateFunctionInitArguments args =
      new AggregateFunctionInitArguments(0, 3);

  @Before
  public void setup() {
    topKFactory = new TopKAggregateFunctionFactory();
    argumentType = Collections.singletonList(SqlArgument.of(SqlTypes.DOUBLE));
  }

  @Test
  public void shouldAggregateTopK() {
    final KsqlAggregateFunction<Object, List<Double>, List<Double>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    List<Double> window = new ArrayList<>();
    for (final Object value : valuesArray) {
      window = topkKudaf.aggregate(value , window);
    }

    assertThat("Invalid results.", window, equalTo(ImmutableList.of(80.0, 60.0, 60.0)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    final KsqlAggregateFunction<Object, List<Double>, List<Double>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    List<Double> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate(10.0, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of(10.0)));
  }

  @Test
  public void shouldMergeTopK() {
    final KsqlAggregateFunction<Object, List<Double>, List<Double>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    final List<Double> array1 = ImmutableList.of(50.0, 45.0, 25.0);
    final List<Double> array2 = ImmutableList.of(60.0, 55.0, 48.0);

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of(60.0, 55.0, 50.0)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final KsqlAggregateFunction<Object, List<Double>, List<Double>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    final List<Double> array1 = ImmutableList.of(50.0, 45.0);
    final List<Double> array2 = ImmutableList.of(60.0);

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of(60.0, 50.0, 45.0)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final KsqlAggregateFunction<Object, List<Double>, List<Double>> topkKudaf =
        topKFactory.createAggregateFunction(argumentType, args);
    final List<Double> array1 = ImmutableList.of(50.0);
    final List<Double> array2 = ImmutableList.of(60.0);

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of(60.0, 50.0)));
  }

}