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

import avro.shaded.com.google.common.collect.Lists;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.List;

@UdafDescription(name = "collect_set", 
    description = "Gather all of the distinct grouped values into a single Array field."
        + " Not available for aggregating values from an input Table."
        + " This version limits the size of the resultant Array to 100000 entries.")
public final class CollectSetUdaf {

  private static final int LIMIT = 100000;

  private CollectSetUdaf() {
    // just to make the checkstyle happy
  }
  
  private static <T> Udaf<T, List<T>> setCollector() {
    return new Udaf<T, List<T>>() {
      @Override
      public List<T> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<T> aggregate(final T thisValue, final List<T> aggregate) {
        if (aggregate.size() < LIMIT && !aggregate.contains(thisValue)) {
          aggregate.add(thisValue);
        }
        return aggregate;
      }

      @Override
      public List<T> merge(final List<T> aggOne, final List<T> aggTwo) {
        for (T thisEntry : aggTwo) {
          if (aggOne.size() < LIMIT && !aggOne.contains(thisEntry)) {
            aggOne.add(thisEntry);
          }
        }
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect distinct values of a Bigint field into a single Array")
  public static Udaf<Long, List<Long>> createCollectSetLong() {
    return setCollector();
  }

  @UdafFactory(description = "collect distinct values of an Integer field into a single Array")
  public static Udaf<Integer, List<Integer>> createCollectSetInt() {
    return setCollector();
  }

  @UdafFactory(description = "collect distinct values of a Double field into a single Array")
  public static Udaf<Double, List<Double>> createCollectSetDouble() {
    return setCollector();
  }

  @UdafFactory(description = "collect distinct values of a String field into a single Array")
  public static Udaf<String, List<String>> createCollectSetString() {
    return setCollector();
  }

  @UdafFactory(description = "collect distinct values of a Boolean field into a single Array")
  public static Udaf<Boolean, List<Boolean>> createCollectSetBool() {
    return setCollector();
  }

}
