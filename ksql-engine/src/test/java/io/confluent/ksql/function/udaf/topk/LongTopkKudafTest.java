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

import java.util.Collections;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class LongTopkKudafTest {
  Object[] valueArray;
  TopKAggregateFunctionFactory topKFactory;
  List<Schema> argumentType;

  @Before
  public void setup() {
    valueArray = new Long[]{10L, 30L, 10L, 45L, 50L, 60L, 20L, 60L, 80L, 35L, 25L};
    topKFactory = new TopKAggregateFunctionFactory(3);
    argumentType = Collections.singletonList(Schema.INT64_SCHEMA);
  }

  @Test
  public void shouldAggregateTopK() {
    KsqlAggregateFunction<Object, Object[]> longTopkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Object[] window = new Long[]{null, null, null};
    for (Object value : valueArray) {
      window = longTopkKudaf.aggregate(value, window);
    }
    assertThat("Invalid results.", window, equalTo(new Long[]{80L, 60L, 60L}));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    KsqlAggregateFunction<Object, Object[]> longTopkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Object[] window = longTopkKudaf.aggregate(80L, new Long[]{null, null, null});
    assertThat("Invalid results.", window, equalTo(new Long[]{80L, null, null}));
  }

  @Test
  public void shouldMergeTopK() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Long[] array1 = new Long[]{50L, 45L, 25L};
    Long[] array2 = new Long[]{60L, 55L, 48l};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new Long[]{60L, 55L, 50L}));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Long[] array1 = new Long[]{50L, 45L, null};
    Long[] array2 = new Long[]{60L, null, null};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new Long[]{60L, 50L, 45L}));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    KsqlAggregateFunction<Object, Object[]> topkKudaf =
            topKFactory.getProperAggregateFunction(argumentType);
    Long[] array1 = new Long[]{50L, null, null};
    Long[] array2 = new Long[]{60L, null, null};

    assertThat("Invalid results.", topkKudaf.getMerger().apply("key", array1, array2),
            equalTo(new Long[]{60L, 50L, null}));
  }
}
