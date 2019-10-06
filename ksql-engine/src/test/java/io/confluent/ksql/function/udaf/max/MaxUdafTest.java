/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.max;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.udaf.Udaf;
import org.junit.Test;

public class MaxUdafTest {

  @Test
  public void shouldMaxLongs() {
    final Udaf<Long, Long, Long> udaf = MaxUdaf.maxLong();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    long currentMax = Long.MIN_VALUE;
    for (final long i: values) {
      currentMax = udaf.aggregate(i, currentMax);
    }
    assertThat(8L, equalTo(currentMax));
  }

  @Test
  public void shouldHandleNullLongs() {
    final Udaf<Long, Long, Long> udaf = MaxUdaf.maxLong();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    Long currentMax = null;

    // null before any aggregation
    currentMax = udaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final long i: values) {
      currentMax = udaf.aggregate(i, currentMax);
    }
    assertThat(8L, equalTo(currentMax));

    // null should not impact result
    currentMax = udaf.aggregate(null, currentMax);
    assertThat(8L, equalTo(currentMax));
  }

  @Test
  public void shouldMergeLongs() {
    final Udaf<Long, Long, Long> udaf = MaxUdaf.maxLong();
    final Long mergeResult1 = udaf.merge(10L, 12L);
    assertThat(mergeResult1, equalTo(12L));
    final Long mergeResult2 = udaf.merge(10L, -12L);
    assertThat(mergeResult2, equalTo(10L));
    final Long mergeResult3 = udaf.merge(-10L, 0L);
    assertThat(mergeResult3, equalTo(0L));
    final Long mergeResult4 = udaf.merge(null, null);
    assertThat(mergeResult4, equalTo(null));
  }

  @Test
  public void shouldMaxIntegers() {
    final Udaf<Integer, Integer, Integer> udaf = MaxUdaf.maxInt();
    final int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    int currentMax = Integer.MIN_VALUE;
    for (final int i: values) {
      currentMax = udaf.aggregate(i, currentMax);
    }
    assertThat(8, equalTo(currentMax));
  }

  @Test
  public void shouldHandleNullIntegers() {
    final Udaf<Integer, Integer, Integer> udaf = MaxUdaf.maxInt();
    final int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    Integer currentMax = null;

    // null before any aggregation
    currentMax = udaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final int i: values) {
      currentMax = udaf.aggregate(i, currentMax);
    }
    assertThat(8, equalTo(currentMax));

    // null should not impact result
    currentMax = udaf.aggregate(null, currentMax);
    assertThat(8, equalTo(currentMax));
  }

  @Test
  public void shouldMergeIntegers() {
    final Udaf<Integer, Integer, Integer> udaf = MaxUdaf.maxInt();
    final Integer mergeResult1 = udaf.merge(10, 12);
    assertThat(mergeResult1, equalTo(12));
    final Integer mergeResult2 = udaf.merge(10, -12);
    assertThat(mergeResult2, equalTo(10));
    final Integer mergeResult3 = udaf.merge( -10, 0);
    assertThat(mergeResult3, equalTo(0));
    final Integer mergeResult4 = udaf.merge(null, null);
    assertThat(mergeResult4, equalTo(null));
  }

  @Test
  public void shouldMaxDoubles() {
    final Udaf<Double, Double, Double> udaf = MaxUdaf.maxDouble();
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    Double currentMax = null;
    for (final double i: values) {
      currentMax = udaf.aggregate(i, currentMax);
    }
    assertThat(8.0, equalTo(currentMax));
  }

  @Test
  public void shouldHandleNullDoubles() {
    final Udaf<Double, Double, Double> udaf = MaxUdaf.maxDouble();
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    Double currentMax = null;

    // aggregate null before any aggregation
    currentMax = udaf.aggregate(null, currentMax);
    assertThat(null, equalTo(currentMax));

    // now send each value to aggregation and verify
    for (final double i: values) {
      currentMax = udaf.aggregate(i, currentMax);
    }
    assertThat(8.0, equalTo(currentMax));

    // null should not impact result
    currentMax = udaf.aggregate(null, currentMax);
    assertThat(8.0, equalTo(currentMax));
  }
  @Test
  public void shouldMergeDoubles() {
    final Udaf<Double, Double, Double> udaf = MaxUdaf.maxDouble();
    final Double mergeResult1 = udaf.merge(10.0, 12.0);
    assertThat(mergeResult1, equalTo(12.0));
    final Double mergeResult2 = udaf.merge(10.0, -12.0);
    assertThat(mergeResult2, equalTo(10.0));
    final Double mergeResult3 = udaf.merge(-10.0, 0.0);
    assertThat(mergeResult3, equalTo(0.0));
    final Double mergeResult4 = udaf.merge(null, null);
    assertThat(mergeResult4, equalTo(null));
  }
}
