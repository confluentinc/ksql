/**
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 **/

package io.confluent.ksql.function.udaf.map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import java.util.Map;
import org.junit.Test;
import io.confluent.ksql.function.udaf.TableUdaf;

public class HistogramUdafTest {

  @Test
  public void shouldCountStrings() {
    TableUdaf<String, Map<String, Long>> udaf = HistogramUdaf.histogramString();
    Map<String, Long> agg = udaf.initialize();
    String[] values = new String[] {"foo", "bar", "foo", "foo", "baz"};
    for (String thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(agg.entrySet(), hasSize(3));
    assertThat(agg, hasEntry("foo", 3L));
    assertThat(agg, hasEntry("bar", 1L));
    assertThat(agg, hasEntry("baz", 1L));
  }

  @Test
  public void shouldMergeIntsIncludingNulls() {
    TableUdaf<Integer, Map<Integer, Long>> udaf = HistogramUdaf.histogramInt();

    Map<Integer, Long> lhs = udaf.initialize();
    Integer[] leftValues = new Integer[] {1, 2, 1, null, 4};
    for (Integer thisValue : leftValues) {
      lhs = udaf.aggregate(thisValue, lhs);
    }
    assertThat(lhs.entrySet(), hasSize(4));
    assertThat(lhs, hasEntry(1, 2L));
    assertThat(lhs, hasEntry(2, 1L));
    assertThat(lhs, hasEntry(null, 1L));
    assertThat(lhs, hasEntry(4, 1L));

    Map<Integer, Long> rhs = udaf.initialize();
    Integer[] rightValues = new Integer[] {1, 3, null, null};
    for (Integer thisValue : rightValues) {
      rhs = udaf.aggregate(thisValue, rhs);
    }
    assertThat(rhs.entrySet(), hasSize(3));
    assertThat(rhs, hasEntry(1, 1L));
    assertThat(rhs, hasEntry(3, 1L));
    assertThat(rhs, hasEntry(null, 2L));

    Map<Integer, Long> merged = udaf.merge(lhs, rhs);
    assertThat(merged.entrySet(), hasSize(5));
    assertThat(merged, hasEntry(1, 3L));
    assertThat(merged, hasEntry(2, 1L));
    assertThat(merged, hasEntry(3, 1L));
    assertThat(merged, hasEntry(4, 1L));
    assertThat(merged, hasEntry(null, 3L));
  }

  @Test
  public void shouldUndoCountedBools() {
    TableUdaf<Boolean, Map<Boolean, Long>> udaf = HistogramUdaf.histogramBool();
    Map<Boolean, Long> agg = udaf.initialize();
    Boolean[] values = new Boolean[] {true, true, false, null, true};
    for (Boolean thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(agg.entrySet(), hasSize(3));
    assertThat(agg, hasEntry(true, 3L));
    assertThat(agg, hasEntry(false, 1L));
    assertThat(agg, hasEntry(null, 1L));

    agg = udaf.undo(true, agg);
    assertThat(agg.entrySet(), hasSize(3));
    assertThat(agg, hasEntry(true, 2L));
    assertThat(agg, hasEntry(false, 1L));
    assertThat(agg, hasEntry(null, 1L));
  }
}
