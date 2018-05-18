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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class DoubleTopkKudafTest {
  ArrayList valueArray;
  TopKAggregateFunctionFactory topKFactory;
  List<Schema> argumentType;

  @Before
  public void setup() {
    valueArray = new ArrayList(Arrays.asList(10.0, 30.0, 45.0, 10.0, 50.0, 60.0, 20.0, 60.0, 80.0, 35.0,
                                             25.0));
    topKFactory = new TopKAggregateFunctionFactory(3);
    argumentType = Collections.singletonList(Schema.FLOAT64_SCHEMA);
  }

  @Test
  public void shouldAggregateTopK() {
    KsqlAggregateFunction<Object, ArrayList> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    ArrayList window = new ArrayList();
    for (Object value : valueArray) {
      window = topkKudaf.aggregate(value , window);
    }

    assertThat("Invalid results.", window, equalTo(Arrays.asList(80.0, 60.0, 60.0)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    KsqlAggregateFunction<Object, ArrayList> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    ArrayList currentVal = new ArrayList();
    currentVal = topkKudaf.aggregate(10.0, currentVal);

    assertThat("Invalid results.", currentVal, equalTo(Arrays.asList(10.0)));
  }

  @Test
  public void shouldMergeTopK() {
    KsqlAggregateFunction<Object, ArrayList> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    ArrayList<Double> array1 = new ArrayList(Arrays.asList(50.0, 45.0, 25.0));
    ArrayList<Double> array2 = new ArrayList(Arrays.asList(60.0, 55.0, 48.0));

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
               equalTo(Arrays.asList(60.0, 55.0, 50.0)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    KsqlAggregateFunction<Object, ArrayList> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    ArrayList<Double> array1 = new ArrayList(Arrays.asList(50.0, 45.0));
    ArrayList<Double> array2 = new ArrayList(Arrays.asList(60.0));

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
               equalTo(Arrays.asList(60.0, 50.0, 45.0)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    KsqlAggregateFunction<Object, ArrayList> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    ArrayList<Double> array1 = new ArrayList(Arrays.asList(50.0));
    ArrayList<Double> array2 = new ArrayList(Arrays.asList(60.0));

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
               equalTo(Arrays.asList(60.0, 50.0)));
  }

}