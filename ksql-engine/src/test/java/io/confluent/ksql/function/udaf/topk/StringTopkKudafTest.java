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

import com.google.common.collect.ImmutableList;

import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import io.confluent.ksql.function.KsqlAggregateFunction;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

@SuppressWarnings("unchecked")
public class StringTopkKudafTest {
  private final List<String> valueArray = ImmutableList.of("10", "ab", "cde", "efg", "aa", "32", "why", "How are you",
      "Test", "123", "432");;
  private TopKAggregateFunctionFactory topKFactory;
  private List<Schema> argumentType;

  @Before
  public void setup() {
    topKFactory = new TopKAggregateFunctionFactory(3);
    argumentType = Collections.singletonList(Schema.OPTIONAL_STRING_SCHEMA);
  }

  @Test
  public void shouldAggregateTopK() {
    KsqlAggregateFunction<String, List<String>> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    List<String> currentVal = new ArrayList<>();
    for (String value : valueArray) {
      currentVal = topkKudaf.aggregate(value , currentVal);
    }

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of("why", "efg", "cde")));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    KsqlAggregateFunction<String, List<String>> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    List<String> currentVal = new ArrayList<>();
    currentVal = topkKudaf.aggregate("why", currentVal);

    assertThat("Invalid results.", currentVal, equalTo(ImmutableList.of("why")));
  }

  @Test
  public void shouldMergeTopK() {
    KsqlAggregateFunction<String, List<String>> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    List<String> array1 = ImmutableList.of("paper", "Hello", "123");
    List<String> array2 = ImmutableList.of("Zzz", "Hi", "456");

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
        equalTo(ImmutableList.of("paper", "Zzz", "Hi")));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    KsqlAggregateFunction<String, List<String>> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    List<String> array1 = ImmutableList.of("50", "45");
    List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
        equalTo(ImmutableList.of("60", "50", "45")));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    KsqlAggregateFunction<String, List<String>> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    List<String> array1 = ImmutableList.of("50");
    List<String> array2 = ImmutableList.of("60");

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
        equalTo(ImmutableList.of("60", "50")));
  }
}