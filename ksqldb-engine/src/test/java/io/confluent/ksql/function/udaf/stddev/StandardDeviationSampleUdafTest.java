/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.stddev;

import static io.confluent.ksql.function.udaf.stddev.StandardDeviationSampleUdaf.stdDevDouble;
import static io.confluent.ksql.function.udaf.stddev.StandardDeviationSampleUdaf.stdDevInt;
import static io.confluent.ksql.function.udaf.stddev.StandardDeviationSampleUdaf.stdDevLong;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.udaf.TableUdaf;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class StandardDeviationSampleUdafTest {

  private static final String COUNT = "COUNT";
  private static final String SUM = "SUM";
  private static final String M2 = "M2";

  @Test
  public void shouldCalculateStdDevLongs() {
    final TableUdaf<Long, Struct, Double> udaf = stdDevLong();
    Struct agg = udaf.initialize();
    final Long[] values = new Long[] {1L, 2L, 3L, 4L, 5L};
    for (final Long thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(agg.getInt64(COUNT), equalTo(5L));
    assertThat(agg.getInt64(SUM), equalTo(15L));
    assertThat(agg.getFloat64(M2), equalTo(10.0));

    final double standardDev = udaf.map(agg);
    assertThat(standardDev, equalTo(1.5811388300841898));
  }

  @Test
  public void shouldCalculateStdDevInts() {
    final TableUdaf<Integer, Struct, Double> udaf = stdDevInt();
    Struct agg = udaf.initialize();
    final Integer[] values = new Integer[] {3, 5, 6, 7};
    for (final Integer thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(agg.getInt64(COUNT), equalTo(4L));
    assertThat(agg.getInt32(SUM), equalTo(21));
    assertThat(agg.getFloat64(M2), equalTo(8.75));

    final double standardDev = udaf.map(agg);
    assertThat(standardDev, equalTo(1.707825127659933));
  }

  @Test
  public void shouldCalculateStdDevDoubles() {
    final TableUdaf<Double, Struct, Double> udaf = stdDevDouble();
    Struct agg = udaf.initialize();
    final Double[] values = new Double[] {10.2, 13.4, 14.5, 17.8};
    for (final Double thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(agg.getInt64(COUNT), equalTo(4L));
    assertThat(agg.getFloat64(SUM), equalTo(55.900000000000006));
    assertThat(agg.getFloat64(M2), equalTo(29.48749999999999));

    final double standardDev = udaf.map(agg);
    assertThat(standardDev, equalTo(3.1351501824739856));
  }

  @Test
  public void shouldAverageZeroes() {
    final TableUdaf<Integer, Struct, Double> udaf = stdDevInt();
    Struct agg = udaf.initialize();
    final int[] values = new int[] {0, 0, 0};
    for (final int thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double standardDev = udaf.map(agg);

    assertThat(standardDev, equalTo(0.0));
  }

  @Test
  public void shouldAverageEmpty() {
    final TableUdaf<Integer, Struct, Double> udaf = stdDevInt();
    final Struct agg = udaf.initialize();
    final double standardDev = udaf.map(agg);

    assertThat(standardDev, equalTo(0.0));
  }

  @Test
  public void shouldIgnoreNull() {
    final TableUdaf<Integer, Struct, Double> udaf = stdDevInt();
    Struct agg = udaf.initialize();
    final Integer[] values = new Integer[] {60, 64, 70};
    for (final int thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    agg = udaf.aggregate(null, agg);
    final double standardDev = udaf.map(agg);
    assertThat(standardDev, equalTo(5.033222956847166));
  }

  @Test
  public void shouldMergeLongs() {
    final TableUdaf<Long, Struct, Double> udaf = stdDevLong();

    Struct left = udaf.initialize();
    final Long[] leftValues = new Long[] {1L, 2L, 3L, 4L, 5L};
    for (final Long thisValue : leftValues) {
      left = udaf.aggregate(thisValue, left);
    }

    Struct right = udaf.initialize();
    final Long[] rightValues = new Long[] {2L, 2L, 1L};
    for (final Long thisValue : rightValues) {
      right = udaf.aggregate(thisValue, right);
    }

    final Struct merged = udaf.merge(left, right);

    assertThat(merged.getInt64(COUNT), equalTo(8L));
    assertThat(merged.getInt64(SUM), equalTo(20L));
    assertThat(merged.getFloat64(M2), equalTo(14.0));

    final double standardDev = udaf.map(merged);
    assertThat(standardDev, equalTo(1.4142135623730951));
  }

  @Test
  public void shouldMergeInts() {
    final TableUdaf<Integer, Struct, Double> udaf = stdDevInt();

    Struct left = udaf.initialize();
    final Integer[] leftValues = new Integer[] {5, 8, 10};
    for (final Integer thisValue : leftValues) {
      left = udaf.aggregate(thisValue, left);
    }

    Struct right = udaf.initialize();
    final Integer[] rightValues = new Integer[] {6, 7, 9};
    for (final Integer thisValue : rightValues) {
      right = udaf.aggregate(thisValue, right);
    }

    final Struct merged = udaf.merge(left, right);

    assertThat(merged.getInt64(COUNT), equalTo(6L));
    assertThat(merged.getInt32(SUM), equalTo(45));
    assertThat(merged.getFloat64(M2), equalTo(17.5));

    final double standardDev = udaf.map(merged);
    assertThat(standardDev, equalTo(1.8708286933869707));
  }

  @Test
  public void shouldMergeDoubles() {
    final TableUdaf<Double, Struct, Double> udaf = stdDevDouble();

    Struct left = udaf.initialize();
    final Double[] leftValues = new Double[] {5.5, 8.4, 10.9};
    for (final Double thisValue : leftValues) {
      left = udaf.aggregate(thisValue, left);
    }

    Struct right = udaf.initialize();
    final Double[] rightValues = new Double[] {6.3, 7.2, 9.7};
    for (final Double thisValue : rightValues) {
      right = udaf.aggregate(thisValue, right);
    }

    final Struct merged = udaf.merge(left, right);

    assertThat(merged.getInt64(COUNT), equalTo(6L));
    assertThat(merged.getFloat64(SUM), equalTo(48.0));
    assertThat(merged.getFloat64(M2), equalTo(21.240000000000006));

    final double standardDev = udaf.map(merged);
    assertThat(standardDev, equalTo(2.0610676844781204));
  }

  @Test
  public void shouldUndoSummedLongs() {
    final TableUdaf<Long, Struct, Double> udaf = stdDevLong();
    Struct agg = udaf.initialize();
    final Long[] values = new Long[] {1L, 2L, 3L, 4L, 5L};
    for (final Long thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(agg.getInt64(COUNT), equalTo(5L));
    assertThat(agg.getInt64(SUM), equalTo(15L));
    assertThat(agg.getFloat64(M2), equalTo(10.0));

    double standardDev = udaf.map(agg);
    assertThat(standardDev, equalTo(1.5811388300841898));

    agg = udaf.undo(2L, agg);

    assertThat(agg.getInt64(COUNT), equalTo(4L));
    assertThat(agg.getInt64(SUM), equalTo(13L));
    assertThat(agg.getFloat64(M2), equalTo(8.75));

    standardDev = udaf.map(agg);
    assertThat(standardDev, equalTo(1.707825127659933));
  }
}
