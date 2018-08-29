/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.function.udaf.array;

import java.util.List;
import avro.shaded.com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;

@UdafDescription(name = "collect_array", 
    description = "Gather all of the grouped values into a single Array field")
public class CollectArrayUdaf {

  @UdafFactory(description = "collect values of a Bigint field into a single Array")
  public static TableUdaf<Long, List<Long>> createCollectArrayLong() {
    return new TableUdaf<Long, List<Long>>() {
      @Override
      public List<Long> undo(final Long valueToUndo, final List<Long> aggregateValue) {
        aggregateValue.remove(aggregateValue.lastIndexOf(valueToUndo));
        return aggregateValue;
      }

      @Override
      public List<Long> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<Long> aggregate(final Long thisValue, final List<Long> aggregate) {
        aggregate.add(thisValue);
        return aggregate;
      }

      @Override
      public List<Long> merge(final List<Long> aggOne, List<Long> aggTwo) {
        aggOne.addAll(aggTwo);
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect values of an Integer field into a single Array")
  public static TableUdaf<Integer, List<Integer>> createCollectArrayInt() {
    return new TableUdaf<Integer, List<Integer>>() {
      @Override
      public List<Integer> undo(final Integer valueToUndo, final List<Integer> aggregateValue) {
        aggregateValue.remove(aggregateValue.lastIndexOf(valueToUndo));
        return aggregateValue;
      }

      @Override
      public List<Integer> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<Integer> aggregate(final Integer thisValue, final List<Integer> aggregate) {
        aggregate.add(thisValue);
        return aggregate;
      }

      @Override
      public List<Integer> merge(final List<Integer> aggOne, final List<Integer> aggTwo) {
        aggOne.addAll(aggTwo);
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect values of a Double field into a single Array")
  public static Udaf<Double, List<Double>> createCollectArrayDouble() {
    return new TableUdaf<Double, List<Double>>() {
      @Override
      public List<Double> undo(final Double valueToUndo, final List<Double> aggregateValue) {
        aggregateValue.remove(aggregateValue.lastIndexOf(valueToUndo));
        return aggregateValue;
      }

      @Override
      public List<Double> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<Double> aggregate(final Double thisValue, final List<Double> aggregate) {
        aggregate.add(thisValue);
        return aggregate;
      }

      @Override
      public List<Double> merge(final List<Double> aggOne, final List<Double> aggTwo) {
        aggOne.addAll(aggTwo);
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect values of a String/Varchar field into a single Array")
  public static Udaf<String, List<String>> createCollectArrayString() {
    return new TableUdaf<String, List<String>>() {
      @Override
      public List<String> undo(final String valueToUndo, final List<String> aggregateValue) {
        aggregateValue.remove(aggregateValue.lastIndexOf(valueToUndo));
        return aggregateValue;
      }

      @Override
      public List<String> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<String> aggregate(final String thisValue, final List<String> aggregate) {
        aggregate.add(thisValue);
        return aggregate;
      }

      @Override
      public List<String> merge(final List<String> aggOne, final List<String> aggTwo) {
        aggOne.addAll(aggTwo);
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect values of a Boolean field into a single Array")
  public static Udaf<Boolean, List<Boolean>> createCollectArrayBool() {
    return new TableUdaf<Boolean, List<Boolean>>() {
      @Override
      public List<Boolean> undo(final Boolean valueToUndo, final List<Boolean> aggregateValue) {
        aggregateValue.remove(aggregateValue.lastIndexOf(valueToUndo));
        return aggregateValue;
      }

      @Override
      public List<Boolean> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<Boolean> aggregate(final Boolean thisValue, final List<Boolean> aggregate) {
        aggregate.add(thisValue);
        return aggregate;
      }

      @Override
      public List<Boolean> merge(final List<Boolean> aggOne, final List<Boolean> aggTwo) {
        aggOne.addAll(aggTwo);
        return aggOne;
      }
    };
  }

}
