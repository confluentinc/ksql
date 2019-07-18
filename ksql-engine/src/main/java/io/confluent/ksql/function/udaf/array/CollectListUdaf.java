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

package io.confluent.ksql.function.udaf.array;

import com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;

@UdafDescription(
    name = "collect_list",
    description = "Gather all of the values from an input grouping into a single Array field."
        + "\nAlthough this aggregate works on both Stream and Table inputs, the order of entries"
        + " in the result array is not guaranteed when working on Table input data."
        + "\nThis version limits the size of the resultant Array to 1000 entries, beyond which"
        + " any further values will be silently ignored.")
public final class CollectListUdaf {

  private static final int LIMIT = 1000;

  private CollectListUdaf() {
    // just to make the checkstyle happy
  }

  private static <T> TableUdaf<T, List<T>> listCollector() {
    return new TableUdaf<T, List<T>>() {

      @Override
      public List<T> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<T> aggregate(final T thisValue, final List<T> aggregate) {
        if (aggregate.size() < LIMIT) {
          aggregate.add(thisValue);
        }
        return aggregate;
      }

      @Override
      public List<T> merge(final List<T> aggOne, final List<T> aggTwo) {
        final int remainingCapacity = LIMIT - aggOne.size();
        aggOne.addAll(aggTwo.subList(0, Math.min(remainingCapacity, aggTwo.size())));
        return aggOne;
      }

      @Override
      public List<T> undo(final T valueToUndo, final List<T> aggregateValue) {
        aggregateValue.remove(aggregateValue.lastIndexOf(valueToUndo));
        return aggregateValue;
      }
    };
  }

  @UdafFactory(description = "collect values of a Bigint field into a single Array")
  public static TableUdaf<Long, List<Long>> createCollectListLong() {
    return listCollector();
  }

  @UdafFactory(description = "collect values of an Integer field into a single Array")
  public static TableUdaf<Integer, List<Integer>> createCollectListInt() {
    return listCollector();
  }

  @UdafFactory(description = "collect values of a Double field into a single Array")
  public static TableUdaf<Double, List<Double>> createCollectListDouble() {
    return listCollector();
  }

  @UdafFactory(description = "collect values of a String/Varchar field into a single Array")
  public static TableUdaf<String, List<String>> createCollectListString() {
    return listCollector();
  }

  @UdafFactory(description = "collect values of a Boolean field into a single Array")
  public static TableUdaf<Boolean, List<Boolean>> createCollectListBool() {
    return listCollector();
  }
}
