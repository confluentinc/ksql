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

package io.confluent.ksql.function.udaf.max;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;
import org.junit.Test;

import java.util.Collections;

import io.confluent.ksql.function.KsqlAggregateFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

public class IntegerMaxKudafTest {

  @Test
  public void shouldFindCorrectMax() {
    IntegerMaxKudaf integerMaxKudaf = getIntegerMaxKudaf();
    int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    int currentMax = Integer.MIN_VALUE;
    for (int i: values) {
      currentMax = integerMaxKudaf.aggregate(i, currentMax);
    }
    assertThat(8, equalTo(currentMax));
  }

  @Test
  public void shouldFindCorrectMaxForMerge() {
    IntegerMaxKudaf integerMaxKudaf = getIntegerMaxKudaf();
    Merger<String, Integer> merger = integerMaxKudaf.getMerger();
    Integer mergeResult1 = merger.apply("Key", 10, 12);
    assertThat(mergeResult1, equalTo(12));
    Integer mergeResult2 = merger.apply("Key", 10, -12);
    assertThat(mergeResult2, equalTo(10));
    Integer mergeResult3 = merger.apply("Key", -10, 0);
    assertThat(mergeResult3, equalTo(0));

  }

  private IntegerMaxKudaf getIntegerMaxKudaf() {
    KsqlAggregateFunction aggregateFunction = new MaxAggFunctionFactory()
        .getProperAggregateFunction(Collections.singletonList(Schema.INT32_SCHEMA));
    assertThat(aggregateFunction, instanceOf(IntegerMaxKudaf.class));
    return  (IntegerMaxKudaf) aggregateFunction;
  }

}
