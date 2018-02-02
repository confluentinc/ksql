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

package io.confluent.ksql.function.udaf.topk;

import io.confluent.ksql.function.KsqlAggregateFunction;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.equalTo;

public class DoubleTopkKudafTest extends TopKKudafBaseTest {

  @Before
  public void setup() {
    valueArray = new Double[]{10.0, 30.0, 45.0, 10.0, 50.0, 60.0, 20.0, 60.0, 80.0, 35.0, 25.0};
    topKFactory = new TopkAggFunctionDeterminer(3);
    argumentType = Arrays.asList(Schema.FLOAT64_SCHEMA);
  }

  @Test
  public void shouldAggregateTopK() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf = getTopKArgumentInstance();
    Object[] window = new Double[]{null, null, null};
    for (Object value : valueArray) {
      window = topkKudaf.aggregate(value , window);
    }

    assertThat("Invalid results.", window, equalTo(new Double[]{80.0, 60.0, 60.0}));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf = getTopKArgumentInstance();
    Object[] currentVal = new Double[]{null, null, null};
    currentVal = topkKudaf.aggregate(10.0, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new Double[]{10.0, null, null}));
  }

  @Test
  public void shouldMergeTopK() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf = getTopKArgumentInstance();
    Double[] array1 = new Double[]{50.0, 45.0, 25.0};
    Double[] array2 = new Double[]{60.0, 55.0, 48.0};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Double[]{60.0, 55.0, 50.0}));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf = getTopKArgumentInstance();
    Double[] array1 = new Double[]{50.0, 45.0, null};
    Double[] array2 = new Double[]{60.0, null, null};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Double[]{60.0, 50.0, 45.0}));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf = getTopKArgumentInstance();
    Double[] array1 = new Double[]{50.0, null, null};
    Double[] array2 = new Double[]{60.0, null, null};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2), equalTo(
        new Double[]{60.0, 50.0, null}));
  }

}
