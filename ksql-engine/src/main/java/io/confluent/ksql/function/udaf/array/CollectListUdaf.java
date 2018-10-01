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

import com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;

@UdafDescription(
    name = "collect_list",
    description = "Gather all the grouped values from an input Stream into a single Array field."
    + " Not available for aggregating values from an input Table."
    + " This version limits the size of the resultant Array to 100000 entries.")
public final class CollectListUdaf {

  private static final int LIMIT = 100000;

  private CollectListUdaf() {
    // just to make the checkstyle happy
  }

  private static <T> TableUdaf<T, List<T>> listCollector() {
    return new TableUdaf<T, List<T>>() {

      public List<T> initialize() {
        return Lists.newArrayList();
      }

      public List<T> aggregate(T thisValue, List<T> aggregate) {
        if (aggregate.size() < LIMIT) {
          aggregate.add(thisValue);
        }
        return aggregate;
      }

      public List<T> merge(List<T> aggOne, List<T> aggTwo) {
        int remainingCapacity = LIMIT - aggOne.size();
        aggOne.addAll(aggTwo.subList(0, Math.min(remainingCapacity, aggTwo.size())));
        return aggOne;
      }

      public List<T> undo(T valueToUndo, List<T> aggregateValue) {
        aggregateValue.remove(aggregateValue.lastIndexOf(valueToUndo));
        return aggregateValue;
      }
    };
  }

  @UdafFactory(description = "collect values of a Bigint field into a single Array")
  public static Udaf<Long, List<Long>> createCollectListLong() {
    return listCollector();
  }

  @UdafFactory(description = "collect values of an Integer field into a single Array")
  public static Udaf<Integer, List<Integer>> createCollectListInt() {
    return listCollector();
  }

  @UdafFactory(description = "collect values of a Double field into a single Array")
  public static Udaf<Double, List<Double>> createCollectListDouble() {
    return listCollector();
  }

  @UdafFactory(description = "collect values of a String/Varchar field into a single Array")
  public static Udaf<String, List<String>> createCollectListString() {
    return listCollector();
  }

  @UdafFactory(description = "collect values of a Boolean field into a single Array")
  public static Udaf<Boolean, List<Boolean>> createCollectListBool() {
    return listCollector();
  }
}
