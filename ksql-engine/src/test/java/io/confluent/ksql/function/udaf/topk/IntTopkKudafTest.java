/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udaf.topk;

import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class IntTopkKudafTest {
  private Object[] valueArray;
  private TopKAggregateFunctionFactory topKFactory;
  private List<Schema> argumentType;

  @Before
  public void setup() {
    valueArray = new Integer[]{10, 30, 45, 10, 50, 60, 20, 60, 80, 35, 25};
    topKFactory = new TopKAggregateFunctionFactory(3);
    argumentType = Collections.singletonList(Schema.INT32_SCHEMA);
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldAggregateTopK() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Object[] currentVal = new Integer[]{null, null, null};
    for (Object value : valueArray) {
      currentVal = topkKudaf.aggregate(value , currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new Integer[]{80, 60, 60}));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Object[] currentVal = new Integer[]{null, null, null};
    currentVal = topkKudaf.aggregate(10, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new Integer[]{10, null, null}));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldMergeTopK() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Integer[] array1 = new Integer[]{50, 45, 25};
    Integer[] array2 = new Integer[]{60, 55, 48};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new Integer[]{60, 55, 50}));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldMergeTopKWithNulls() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Integer[] array1 = new Integer[]{50, 45, null};
    Integer[] array2 = new Integer[]{60, null, null};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new Integer[]{60, 50, 45}));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldMergeTopKWithMoreNulls() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Integer[] array1 = new Integer[]{50, null, null};
    Integer[] array2 = new Integer[]{60, null, null};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new Integer[]{60, 50, null}));
  }

  @SuppressWarnings("unchecked")
  @Test
  public void shouldAggregateAndProducedOrderedTopK() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);

    Object[] aggregate = topkKudaf.aggregate(1, new Object[3]);
    assertThat(aggregate, equalTo(new Integer[] {1, null, null}));
    Object[] agg2 = topkKudaf.aggregate(100, new Integer[] {1, null, null});
    assertThat(agg2, equalTo(new Integer[] {100, 1, null}));
  }
}
