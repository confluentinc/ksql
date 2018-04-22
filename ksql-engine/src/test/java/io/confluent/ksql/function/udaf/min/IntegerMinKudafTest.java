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

package io.confluent.ksql.function.udaf.min;


import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

import java.util.Collections;

import io.confluent.ksql.function.KsqlAggregateFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class IntegerMinKudafTest {

  @Test
  public void shouldFindCorrectMin() {
    IntegerMinKudaf integerMinKudaf = getIntegerMinKudaf();
    int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    int currentMin = Integer.MAX_VALUE;
    for (int i: values) {
      currentMin = integerMinKudaf.aggregate(i, currentMin);
    }
    assertThat(2, equalTo(currentMin));
  }

  @Test
  public void shouldFindCorrectMinForMerge() {
    IntegerMinKudaf integerMinKudaf = getIntegerMinKudaf();
    Merger<String, Integer> merger = integerMinKudaf.getMerger();
    Integer mergeResult1 = merger.apply("Key", 10, 12);
    assertThat(mergeResult1, equalTo(10));
    Integer mergeResult2 = merger.apply("Key", 10, -12);
    assertThat(mergeResult2, equalTo(-12));
    Integer mergeResult3 = merger.apply("Key", -10, 0);
    assertThat(mergeResult3, equalTo(-10));

  }


  private IntegerMinKudaf getIntegerMinKudaf() {
    KsqlAggregateFunction aggregateFunction = new MinAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));
    assertThat(aggregateFunction, instanceOf(IntegerMinKudaf.class));
    return  (IntegerMinKudaf) aggregateFunction;
  }
}
