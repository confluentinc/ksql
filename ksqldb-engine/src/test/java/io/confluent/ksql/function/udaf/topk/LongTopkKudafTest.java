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
import static org.hamcrest.MatcherAssert.assertThat;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

@SuppressWarnings("unchecked")
public class LongTopkKudafTest {
  private final List<Long> valuesArray = ImmutableList.of(10L, 30L, 10L, 45L, 50L, 60L, 20L, 60L, 80L, 35L,
      25L);

  @Test
  public void shouldAggregateTopK() {
    final Udaf<Long, List<Long>, List<Long>> longTopkKudaf = createUdaf();
    List<Long> window = new ArrayList<>();
    for (final Long value : valuesArray) {
      window = longTopkKudaf.aggregate(value, window);
    }
    assertThat("Invalid results.", window, equalTo(ImmutableList.of(80L, 60L, 60L)));
  }

  @Test
  public void shouldAggregateTopKWithLessThanKValues() {
    final Udaf<Long, List<Long>, List<Long>> longTopkKudaf = createUdaf();
    final List<Long> window = longTopkKudaf.aggregate(80L, new ArrayList());
    assertThat("Invalid results.", window, equalTo(ImmutableList.of(80L)));
  }

  @Test
  public void shouldMergeTopK() {
    final Udaf<Long, List<Long>, List<Long>> topkKudaf = createUdaf();
    final List<Long> array1 = ImmutableList.of(50L, 45L, 25L);
    final List<Long> array2 = ImmutableList.of(60L, 55L, 48L);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60L, 55L, 50L)));
  }

  @Test
  public void shouldMergeTopKWithNulls() {
    final Udaf<Long, List<Long>, List<Long>> topkKudaf = createUdaf();
    final List<Long> array1 = ImmutableList.of(50L, 45L);
    final List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60L, 50L, 45L)));
  }

  @Test
  public void shouldMergeTopKWithMoreNulls() {
    final Udaf<Long, List<Long>, List<Long>> topkKudaf = createUdaf();
    final List<Long> array1 = ImmutableList.of(50L);
    final List<Long> array2 = ImmutableList.of(60L);

    assertThat("Invalid results.", topkKudaf.merge(array1, array2),
        equalTo(ImmutableList.of(60L, 50L)));
  }

  private Udaf<Long, List<Long>, List<Long>> createUdaf() {
    Udaf<Long, List<Long>, List<Long>> udaf = TopkKudaf.createTopKLong(3);
    udaf.initializeTypeArguments(Collections.singletonList(SqlArgument.of(SqlTypes.BIGINT)));
    return udaf;
  }
}