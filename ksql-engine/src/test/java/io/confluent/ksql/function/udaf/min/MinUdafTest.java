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

package io.confluent.ksql.function.udaf.min;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.udaf.Udaf;
import org.junit.Test;

public class MinUdafTest {

  @Test
  public void shouldMinLongs() {
    final Udaf<Long, Long, Long> udaf = MinUdaf.minLong();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    long currentMin = Long.MAX_VALUE;
    for (final long i: values) {
      currentMin = udaf.aggregate(i, currentMin);
    }
    assertThat(2L, equalTo(currentMin));
  }

  @Test
  public void shouldHandleNullLongs() {
    final Udaf<Long, Long, Long> udaf = MinUdaf.minLong();
    final long[] values = new long[]{3L, 5L, 8L, 2L, 3L, 4L, 5L};
    Long currentMin = udaf.initialize();

    // null before any aggregation
    currentMin = udaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final long i: values) {
      currentMin = udaf.aggregate(i, currentMin);
    }
    assertThat(2L, equalTo(currentMin));

    // null should not impact result
    currentMin = udaf.aggregate(null, currentMin);
    assertThat(2L, equalTo(currentMin));
  }

  @Test
  public void shouldMergeLongs() {
    final Udaf<Long, Long, Long> udaf = MinUdaf.minLong();
    final Long mergeResult1 = udaf.merge(10L, 12L);
    assertThat(mergeResult1, equalTo(10L));
    final Long mergeResult2 = udaf.merge(10L, -12L);
    assertThat(mergeResult2, equalTo(-12L));
    final Long mergeResult3 = udaf.merge(-10L, 0L);
    assertThat(mergeResult3, equalTo(-10L));
  }

  @Test
  public void shouldMinIntegers() {
    final Udaf<Integer, Integer, Integer> udaf = MinUdaf.minInt();
    final int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    int currentMin = Integer.MAX_VALUE;
    for (final int i: values) {
      currentMin = udaf.aggregate(i, currentMin);
    }
    assertThat(2, equalTo(currentMin));
  }

  @Test
  public void shouldHandleNullIntegers() {
    final Udaf<Integer, Integer, Integer> udaf = MinUdaf.minInt();
    final int[] values = new int[]{3, 5, 8, 2, 3, 4, 5};
    Integer currentMin = udaf.initialize();

    // null before any aggregation
    currentMin = udaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final int i: values) {
      currentMin = udaf.aggregate(i, currentMin);
    }
    assertThat(2, equalTo(currentMin));

    // null should not impact result
    currentMin = udaf.aggregate(null, currentMin);
    assertThat(2, equalTo(currentMin));
  }

  @Test
  public void shouldMergeIntegers() {
    final Udaf<Integer, Integer, Integer> udaf = MinUdaf.minInt();
    final Integer mergeResult1 = udaf.merge(10, 12);
    assertThat(mergeResult1, equalTo(10));
    final Integer mergeResult2 = udaf.merge(10, -12);
    assertThat(mergeResult2, equalTo(-12));
    final Integer mergeResult3 = udaf.merge( -10, 0);
    assertThat(mergeResult3, equalTo(-10));
  }

  @Test
  public void shouldMinDoubles() {
    final Udaf<Double, Double, Double> udaf = MinUdaf.minDouble();
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    double currentMin = Double.MAX_VALUE;
    for (final double i: values) {
      currentMin = udaf.aggregate(i, currentMin);
    }
    assertThat(2.2, equalTo(currentMin));
  }

  @Test
  public void shouldHandleNullDoubles() {
    final Udaf<Double, Double, Double> udaf = MinUdaf.minDouble();
    final double[] values = new double[]{3.0, 5.0, 8.0, 2.2, 3.5, 4.6, 5.0};
    Double currentMin = udaf.initialize();

    // aggregate null before any aggregation
    currentMin = udaf.aggregate(null, currentMin);
    assertThat(null, equalTo(currentMin));

    // now send each value to aggregation and verify
    for (final double i: values) {
      currentMin = udaf.aggregate(i, currentMin);
    }
    assertThat(2.2, equalTo(currentMin));

    // null should not impact result
    currentMin = udaf.aggregate(null, currentMin);
    assertThat(2.2, equalTo(currentMin));
  }
  @Test
  public void shouldMergeDoubles() {
    final Udaf<Double, Double, Double> udaf = MinUdaf.minDouble();
    final Double mergeResult1 = udaf.merge(10.0, 12.0);
    assertThat(mergeResult1, equalTo(10.0));
    final Double mergeResult2 = udaf.merge(10.0, -12.0);
    assertThat(mergeResult2, equalTo(-12.0));
    final Double mergeResult3 = udaf.merge(-10.0, 0.0);
    assertThat(mergeResult3, equalTo(-10.0));
    final Double mergeResult4 = udaf.merge(null, null);
    assertThat(mergeResult4, equalTo(null));
  }
}
