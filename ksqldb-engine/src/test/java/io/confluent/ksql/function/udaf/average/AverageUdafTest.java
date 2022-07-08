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

package io.confluent.ksql.function.udaf.average;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

import io.confluent.ksql.function.udaf.TableUdaf;
import org.apache.kafka.connect.data.Struct;
import org.junit.Test;

public class AverageUdafTest {

  private static final String COUNT = "COUNT";
  private static final String SUM = "SUM";

  @Test
  public void shouldAggregateLongs() {
    final TableUdaf<Long, Struct, Double> udaf = AverageUdaf.averageLong();
    Struct agg = udaf.initialize();
    final Long[] values = new Long[] {1L, 1L, 1L, 1L, 1L};
    for (final Long thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(5L, equalTo(agg.getInt64("COUNT")));
    assertThat(5L, equalTo(agg.getInt64("SUM")));
  }

  @Test
  public void shouldAverageLongs() {
    final TableUdaf<Long, Struct, Double> udaf = AverageUdaf.averageLong();
    Struct agg = udaf.initialize();
    final long[] values = new long[] {1L, 1L, 1L, 1L, 1L};
    for (final long thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double avg = udaf.map(agg);

    assertThat(1.0, equalTo(avg));
  }

  @Test
  public void shouldAverageInts() {
    final TableUdaf<Integer, Struct, Double> udaf = AverageUdaf.averageInt();
    Struct agg = udaf.initialize();
    final int[] values = new int[] {1, 1, 1, 1, 1};
    for (final int thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double avg = udaf.map(agg);

    assertThat(1.0, equalTo(avg));
  }

  @Test
  public void shouldAverageDoubles() {
    final TableUdaf<Double, Struct, Double> udaf = AverageUdaf.averageDouble();
    Struct agg = udaf.initialize();
    final double[] values = new double[] {1.0, 1.0, 1.0, 1.0, 1.0};
    for (final double thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double avg = udaf.map(agg);

    assertThat(1.0, equalTo(avg));
  }

  @Test
  public void shouldAverageZeroes() {
    final TableUdaf<Integer, Struct, Double> udaf = AverageUdaf.averageInt();
    Struct agg = udaf.initialize();
    final int[] values = new int[] {0, 0, 0};
    for (final int thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    final double avg = udaf.map(agg);

    assertThat(0.0, equalTo(avg));
  }

  @Test
  public void shouldAverageEmpty() {
    final TableUdaf<Integer, Struct, Double> udaf = AverageUdaf.averageInt();
    final Struct agg = udaf.initialize();
    final double avg = udaf.map(agg);

    assertThat(0.0, equalTo(avg));
  }

  @Test
  public void shouldIgnoreNull() {
    final TableUdaf<Integer, Struct, Double> udaf = AverageUdaf.averageInt();
    Struct agg = udaf.initialize();
    final Integer[] values = new Integer[] {1, 1, 1};
    for (final int thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    agg = udaf.aggregate(null, agg);
    final double avg = udaf.map(agg);

    assertThat(1.0, equalTo(avg));
  }

  @Test
  public void shouldMergeAverages() {
    final TableUdaf<Long, Struct, Double> udaf = AverageUdaf.averageLong();

    Struct left = udaf.initialize();
    final Long[] leftValues = new Long[]  {1L, 1L, 1L, 1L, 1L};
    for (final Long thisValue : leftValues) {
      left = udaf.aggregate(thisValue, left);
    }

    Struct right = udaf.initialize();
    final Long[] rightValues = new Long[] {2L, 2L, 1L};
    for (final Long thisValue : rightValues) {
      right = udaf.aggregate(thisValue, right);
    }


    final Struct merged = udaf.merge(left, right);
    assertThat(8L, equalTo(merged.getInt64(COUNT)));
    assertThat(10L, equalTo(merged.getInt64(SUM)));

  }

  @Test
  public void shouldUndoSummedCountedValues() {
    final TableUdaf<Long, Struct, Double> udaf = AverageUdaf.averageLong();
    Struct agg = udaf.initialize();
    final Long[] values = new Long[] {1L, 1L, 1L, 1L, 1L};
    for (final Long thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }

    agg = udaf.undo(1L, agg);
    assertThat(4L, equalTo(agg.getInt64(COUNT)));
    assertThat(4L, equalTo(agg.getInt64(SUM)));
  }

  @Test
  public void undoShouldHandleNull() {
    final TableUdaf<Long, Struct, Double> udaf = AverageUdaf.averageLong();
    Struct agg = udaf.initialize();
    final Long[] values = new Long[] {1L, 1L, 1L, 1L, null};
    for (final Long thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }

    agg = udaf.undo(null, agg);
    assertThat(4L, equalTo(agg.getInt64(COUNT)));
    assertThat(4L, equalTo(agg.getInt64(SUM)));
  }

}
