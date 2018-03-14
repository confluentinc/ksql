/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udaf.topkdistinct;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DoubleTopkDistinctKudafTest {

  Double[] valueArray;
  private final TopkDistinctKudaf<Double> doubleTopkDistinctKudaf
          = new TopkDistinctKudaf<>(0, 3, Schema.FLOAT64_SCHEMA, Double.class);

  @Before
  public void setup() {
    valueArray = new Double[]{10.0, 30.0, 45.0, 10.0, 50.0, 60.0, 20.0, 60.0, 80.0, 35.0, 25.0,
                              60.0, 80.0};

  }

  @Test
  public void shouldAggregateTopK() {
    Double[] currentVal = new Double[]{null, null, null};
    for (Double d: valueArray) {
      currentVal = doubleTopkDistinctKudaf.aggregate(d, currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new Double[]{80.0, 60.0, 50.0}));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    Double[] currentVal = new Double[]{null, null, null};
    currentVal = doubleTopkDistinctKudaf.aggregate(80.0, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new Double[]{80.0, null, null}));
  }

  @Test
  public void shouldMergeTopK() {
    Double[] array1 = new Double[]{50.0, 45.0, 25.0};
    Double[] array2 = new Double[]{60.0, 50.0, 48.0};

    assertThat("Invalid results.", doubleTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Double[]{60.0, 50.0, 48.0}));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    Double[] array1 = new Double[]{50.0, 45.0, null};
    Double[] array2 = new Double[]{60.0, null, null};

    assertThat("Invalid results.", doubleTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Double[]{60.0, 50.0, 45.0}));
  }

  @Test
  public void shouldMergeTopKWithNullsDuplicates() {
    Double[] array1 = new Double[]{50.0, 45.0, null};
    Double[] array2 = new Double[]{60.0, 50.0, null};

    assertThat("Invalid results.", doubleTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Double[]{60.0, 50.0, 45.0}));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    Double[] array1 = new Double[]{60.0, null, null};
    Double[] array2 = new Double[]{60.0, null, null};

    assertThat("Invalid results.", doubleTopkDistinctKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Double[]{60.0, null, null}));
  }
}
