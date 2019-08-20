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
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.kafka.connect.data.Schema;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class LongTopkKudafTest {
  private final List<Long> valuesArray = ImmutableList.of(10L, 30L, 10L, 45L, 50L, 60L, 20L, 60L, 80L, 35L,
      25L);
  private TopKAggregateFunctionFactory topKFactory;
  private List<Schema> argumentType;

  @Before
  public void setup() {
    topKFactory = new TopKAggregateFunctionFactory(3);
    argumentType = Collections.singletonList(Schema.OPTIONAL_INT64_SCHEMA);
  }

  @Test
  public void shouldAggregateTopK() {
    final KsqlAggregateFunction<Long, List<Long>> longTopkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    List<Long> window = new ArrayList<>();
    for (final Long value : valuesArray) {
      window = longTopkKudaf.aggregate(value, window);
    }
    assertThat("Invalid results.", window, equalTo(ImmutableList.of(80L, 60L, 60L)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    final KsqlAggregateFunction<Long, List<Long>> longTopkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    final List<Long> window = longTopkKudaf.aggregate(80L, new ArrayList());
    assertThat("Invalid results.", window, equalTo(ImmutableList.of(80L)));
  }

  @Test
  public void shouldMergeTopK() {
    final KsqlAggregateFunction<Long, List<Long>> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    final List<Long> array1 = ImmutableList.of(50L, 45L, 25L);
    final List<Long> array2 = ImmutableList.of(60L, 55L, 48L);

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of(60L, 55L, 50L)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final KsqlAggregateFunction<Long, List<Long>> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    final List<Long> array1 = ImmutableList.of(50L, 45L);
    final List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of(60L, 50L, 45L)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final KsqlAggregateFunction<Long, List<Long>> topkKudaf =
        topKFactory.getProperAggregateFunction(argumentType);
    final List<Long> array1 = ImmutableList.of(50L);
    final List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", topkKudaf.getMerger().apply(null, array1, array2),
        equalTo(ImmutableList.of(60L, 50L)));
  }
}