/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.map;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.function.udaf.TableUdaf;
import java.util.Map;
import org.junit.Test;

public class HistogramUdafTest {

  @Test
  public void shouldCountStrings() {
    final TableUdaf<String, Map<String, Long>> udaf = HistogramUdaf.histogramString();
    Map<String, Long> agg = udaf.initialize();
    final String[] values = new String[] {"foo", "bar", "foo", "foo", "baz"};
    for (final String thisValue : values) {
      agg = udaf.aggregate(thisValue, agg);
    }
    assertThat(agg.entrySet(), hasSize(3));
    assertThat(agg, hasEntry("foo", 3L));
    assertThat(agg, hasEntry("bar", 1L));
    assertThat(agg, hasEntry("baz", 1L));
  }

  @Test
  public void shouldMergeCountsIncludingNulls() {
    final TableUdaf<String, Map<String, Long>> udaf = HistogramUdaf.histogramString();

    Map<String, Long> lhs = udaf.initialize();
    final Integer[] leftValues = new Integer[] {1, 2, 1, 4};
    for (final Integer thisValue : leftValues) {
      lhs = udaf.aggregate(String.valueOf(thisValue), lhs);
    }
    lhs = udaf.aggregate(null, lhs);
    assertThat(lhs.entrySet(), hasSize(4));
    assertThat(lhs, hasEntry("1", 2L));
    assertThat(lhs, hasEntry("2", 1L));
    assertThat(lhs, hasEntry("4", 1L));
    assertThat(lhs, hasEntry(null, 1L));

    Map<String, Long> rhs = udaf.initialize();
    final Integer[] rightValues = new Integer[] {1, 3};
    for (final Integer thisValue : rightValues) {
      rhs = udaf.aggregate(String.valueOf(thisValue), rhs);
    }
    rhs = udaf.aggregate(null, rhs);
    rhs = udaf.aggregate(null, rhs);
    assertThat(rhs.entrySet(), hasSize(3));
    assertThat(rhs, hasEntry("1", 1L));
    assertThat(rhs, hasEntry("3", 1L));
    assertThat(rhs, hasEntry(null, 2L));

    final Map<String, Long> merged = udaf.merge(lhs, rhs);
    assertThat(merged.entrySet(), hasSize(5));
    assertThat(merged, hasEntry("1", 3L));
    assertThat(merged, hasEntry("2", 1L));
    assertThat(merged, hasEntry("3", 1L));
    assertThat(merged, hasEntry("4", 1L));
    assertThat(merged, hasEntry(null, 3L));
  }

  @Test
  public void shouldUndoCountedValues() {
    final TableUdaf<String, Map<String, Long>> udaf = HistogramUdaf.histogramString();
    Map<String, Long> agg = udaf.initialize();
    final Boolean[] values = new Boolean[] {true, true, false, null, true};
    for (final Boolean thisValue : values) {
      agg = udaf.aggregate(String.valueOf(thisValue), agg);
    }
    assertThat(agg.entrySet(), hasSize(3));
    assertThat(agg, hasEntry("true", 3L));
    assertThat(agg, hasEntry("false", 1L));
    assertThat(agg, hasEntry("null", 1L));

    agg = udaf.undo("true", agg);
    assertThat(agg.entrySet(), hasSize(3));
    assertThat(agg, hasEntry("true", 2L));
    assertThat(agg, hasEntry("false", 1L));
    assertThat(agg, hasEntry("null", 1L));
  }

  @Test
  public void shouldNotExceedSizeLimit() {
    final TableUdaf<String, Map<String, Long>> udaf = HistogramUdaf.histogramString();
    Map<String, Long> agg = udaf.initialize();
    for (int thisValue = 1; thisValue < 2500; thisValue++) {
      agg = udaf.aggregate(String.valueOf(thisValue), agg);
    }
    assertThat(agg.entrySet(), hasSize(1000));
    assertThat(agg, hasEntry("1", 1L));
    assertThat(agg, hasEntry("1000", 1L));
    assertThat(agg, not(hasEntry("1001", 1L)));
  }

}
