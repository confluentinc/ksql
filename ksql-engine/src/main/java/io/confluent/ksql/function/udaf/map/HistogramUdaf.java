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

@UdafDescription(name = "histogram", description = "Returns a map of each input value and how many"
    + " times each occurs. Not applicable for complex types (map, array, or struct).")
public class HistogramUdaf {

  @UdafFactory(description = "See above.")
  public static TableUdaf<String, Map<String, Long>> histogramString() {
    return new TableUdaf<String, Map<String, Long>>() {

      @Override
      public Map<String, Long> initialize() {
        return Maps.newHashMap();
      }

      @Override
      public Map<String, Long> aggregate(final String current, final Map<String, Long> aggregate) {
        aggregate.merge(current, 1L, Long::sum);
        return aggregate;
      }

      @Override
      public Map<String, Long> merge(final Map<String, Long> agg1, final Map<String, Long> agg2) {
        agg1.putAll(agg2); // TODO safe to modify input aggregate value ? or should take a copy?
        return agg1;
      }

      @Override
      public Map<String, Long> undo(final String valueToUndo, final Map<String, Long> aggregate) {
        aggregate.compute(valueToUndo, (k, v) -> (--v < 1) ? null : v);
        return aggregate;
      }
    };
  }

  @UdafFactory(description = "See above.")
  public static TableUdaf<Boolean, Map<Boolean, Long>> histogramBool() {
    return new TableUdaf<Boolean, Map<Boolean, Long>>() {

      @Override
      public Map<Boolean, Long> initialize() {
        return Maps.newHashMap();
      }

      @Override
      public Map<Boolean, Long> aggregate(final Boolean current, 
          final Map<Boolean, Long> aggregate) {
        aggregate.merge(current, 1L, Long::sum);
        return aggregate;
      }

      @Override
      public Map<Boolean, Long> merge(final Map<Boolean, Long> agg1, 
          final Map<Boolean, Long> agg2) {
        agg1.putAll(agg2);
        return agg1;
      }

      @Override
      public Map<Boolean, Long> undo(final Boolean valueToUndo, 
          final Map<Boolean, Long> aggregate) {
        aggregate.compute(valueToUndo, (k, v) -> (--v < 1) ? null : v);
        return aggregate;
      }
    };
  }

  @UdafFactory(description = "See above.")
  public static TableUdaf<Integer, Map<Integer, Long>> histogramInt() {
    return new TableUdaf<Integer, Map<Integer, Long>>() {

      @Override
      public Map<Integer, Long> initialize() {
        return Maps.newHashMap();
      }

      @Override
      public Map<Integer, Long> aggregate(final Integer current, 
          final Map<Integer, Long> aggregate) {
        aggregate.merge(current, 1L, Long::sum);
        return aggregate;
      }

      @Override
      public Map<Integer, Long> merge(final Map<Integer, Long> agg1, 
          final Map<Integer, Long> agg2) {
        agg1.putAll(agg2);
        return agg1;
      }

      @Override
      public Map<Integer, Long> undo(final Integer valueToUndo, 
          final Map<Integer, Long> aggregate) {
        aggregate.compute(valueToUndo, (k, v) -> (--v < 1) ? null : v);
        return aggregate;
      }
    };
  }

  @UdafFactory(description = "See above.")
  public static TableUdaf<Long, Map<Long, Long>> histogramLong() {
    return new TableUdaf<Long, Map<Long, Long>>() {

      @Override
      public Map<Long, Long> initialize() {
        return Maps.newHashMap();
      }

      @Override
      public Map<Long, Long> aggregate(final Long current, final Map<Long, Long> aggregate) {
        aggregate.merge(current, 1L, Long::sum);
        return aggregate;
      }

      @Override
      public Map<Long, Long> merge(final Map<Long, Long> agg1, final Map<Long, Long> agg2) {
        agg1.putAll(agg2);
        return agg1;
      }

      @Override
      public Map<Long, Long> undo(final Long valueToUndo, final Map<Long, Long> aggregate) {
        aggregate.compute(valueToUndo, (k, v) -> (--v < 1) ? null : v);
        return aggregate;
      }
    };
  }

  @UdafFactory(description = "See above.")
  public static TableUdaf<Double, Map<Double, Long>> histogramDouble() {
    return new TableUdaf<Double, Map<Double, Long>>() {

      @Override
      public Map<Double, Long> initialize() {
        return Maps.newHashMap();
      }

      @Override
      public Map<Double, Long> aggregate(final Double current, final Map<Double, Long> aggregate) {
        aggregate.merge(current, 1L, Long::sum);
        return aggregate;
      }

      @Override
      public Map<Double, Long> merge(final Map<Double, Long> agg1, final Map<Double, Long> agg2) {
        agg1.putAll(agg2);
        return agg1;
      }

      @Override
      public Map<Double, Long> undo(final Double valueToUndo, final Map<Double, Long> aggregate) {
        aggregate.compute(valueToUndo, (k, v) -> (--v < 1) ? null : v);
        return aggregate;
      }
    };
  }


}
