/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package io.confluent.ksql.function.udaf.map;

import com.google.common.collect.Maps;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.Map;

@UdafDescription(name = "histogram", description = "Returns a map of each distinct value from the"
    + " input Stream or Table and how many times each occurs."
    + " \nNot applicable for complex types (map, array, or struct)."
    + " \nThis version limits the size of the resultant Map to 1000 entries. Any entries added"
    + " beyond this limit will be ignored.")
public final class HistogramUdaf {

  private static final int LIMIT = 1000;
  
  private HistogramUdaf() {
  }
  
  private static <T> TableUdaf<T, Map<T, Long>> histogram() {
    return new TableUdaf<T, Map<T, Long>>() {

      @Override
      public Map<T, Long> initialize() {
        return Maps.newHashMap();
      }

      @Override
      public Map<T, Long> aggregate(final T current, final Map<T, Long> aggregate) {
        aggregate.merge(current, aggregate.size() < LIMIT ? 1L : null, Long::sum);
        return aggregate;
      }

      @Override
      public Map<T, Long> merge(final Map<T, Long> agg1, final Map<T, Long> agg2) {
        agg2.forEach((k, v) -> agg1.merge(k, agg1.size() < LIMIT ? v : null, Long::sum));
        return agg1;
      }

      @Override
      public Map<T, Long> undo(final T valueToUndo, final Map<T, Long> aggregate) {
        aggregate.compute(valueToUndo, (k, v) -> (--v < 1) ? null : v);
        return aggregate;
      }
    };
  }

  @UdafFactory(description = "Build a value-to-count histogram of input Strings")
  public static TableUdaf<String, Map<String, Long>> histogramString() {
    return histogram();
  }

  @UdafFactory(description = "Build a value-to-count histogram of input Booleans")
  public static TableUdaf<Boolean, Map<Boolean, Long>> histogramBool() {
    return histogram();
  }

  @UdafFactory(description = "Build a value-to-count histogram of input Integers")
  public static TableUdaf<Integer, Map<Integer, Long>> histogramInt() {
    return histogram();
  }

  @UdafFactory(description = "Build a value-to-count histogram of input Bigints")
  public static TableUdaf<Long, Map<Long, Long>> histogramLong() {
    return histogram();
  }

  @UdafFactory(description = "Build a value-to-count histogram of input Doubles")
  public static TableUdaf<Double, Map<Double, Long>> histogramDouble() {
    return histogram();
  }

}
