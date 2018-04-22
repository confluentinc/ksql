/*
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

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import io.confluent.ksql.function.KsqlAggregateFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class StringTopkKudafTest {
  private Object[] valueArray;
  private TopKAggregateFunctionFactory topKFactory;
  private List<Schema> argumentType;

  @Before
  public void setup() {
    valueArray = new String[]{"10", "ab", "cde", "efg", "aa", "32", "why", "How are you", "Test",
                              "123", "432"};
    topKFactory = new TopKAggregateFunctionFactory(3);
    argumentType = Collections.singletonList(Schema.STRING_SCHEMA);
  }

  @Test
  public void shouldAggregateTopK() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Object[] currentVal = new String[]{null, null, null};
    for (Object value : valueArray) {
      currentVal = topkKudaf.aggregate(value , currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(new String[]{"why", "efg", "cde"}));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Object[] currentVal = new String[]{null, null, null};
    currentVal = topkKudaf.aggregate("why", currentVal);

    assertThat("Invalid results.", currentVal, equalTo(new String[]{"why", null, null}));
  }

  @Test
  public void shouldMergeTopK() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    String[] array1 = new String[]{"paper", "Hello", "123"};
    String[] array2 = new String[]{"Zzz", "Hi", "456"};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new String[]{"paper", "Zzz", "Hi"}));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    String[] array1 = new String[]{"50", "45", null};
    String[] array2 = new String[]{"60", null, null};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new String[]{"60", "50", "45"}));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    String[] array1 = new String[]{"50", null, null};
    String[] array2 = new String[]{"60", null, null};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new String[]{"60", "50", null}));
  }
}
