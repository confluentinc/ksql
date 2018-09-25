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
    description = "Gather all of the distinct grouped values into a single Array field")
public final class CollectSetUdaf {

  private CollectSetUdaf() {
    // just to make the checkstyle happy
  }
  
  @UdafFactory(description = "collect distinct values of a Bigint field into a single Array")
  public static Udaf<Long, List<Long>> createCollectSetLong() {
    return new Udaf<Long, List<Long>>() {
      @Override
      public List<Long> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<Long> aggregate(final Long thisValue, final List<Long> aggregate) {
        if (!aggregate.contains(thisValue)) {
          aggregate.add(thisValue);
        }
        return aggregate;
      }

      @Override
      public List<Long> merge(final List<Long> aggOne, final List<Long> aggTwo) {
        for (Long thisEntry : aggTwo) {
          if (!aggOne.contains(thisEntry)) {
            aggOne.add(thisEntry);
          }
        }
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect distinct values of an Integer field into a single Array")
  public static Udaf<Integer, List<Integer>> createCollectSetInt() {
    return new Udaf<Integer, List<Integer>>() {
      @Override
      public List<Integer> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<Integer> aggregate(final Integer thisValue, final List<Integer> aggregate) {
        if (!aggregate.contains(thisValue)) {
          aggregate.add(thisValue);
        }
        return aggregate;
      }

      @Override
      public List<Integer> merge(final List<Integer> aggOne, final List<Integer> aggTwo) {
        for (Integer thisEntry : aggTwo) {
          if (!aggOne.contains(thisEntry)) {
            aggOne.add(thisEntry);
          }
        }
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect distinct values of a Double field into a single Array")
  public static Udaf<Double, List<Double>> createCollectSetDouble() {
    return new Udaf<Double, List<Double>>() {
      @Override
      public List<Double> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<Double> aggregate(final Double thisValue, final List<Double> aggregate) {
        if (!aggregate.contains(thisValue)) {
          aggregate.add(thisValue);
        }
        return aggregate;
      }

      @Override
      public List<Double> merge(final List<Double> aggOne, final List<Double> aggTwo) {
        for (Double thisEntry : aggTwo) {
          if (!aggOne.contains(thisEntry)) {
            aggOne.add(thisEntry);
          }
        }
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect distinct values of a String field into a single Array")
  public static Udaf<String, List<String>> createCollectSetString() {
    return new Udaf<String, List<String>>() {
      @Override
      public List<String> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<String> aggregate(final String thisValue, final List<String> aggregate) {
        if (!aggregate.contains(thisValue)) {
          aggregate.add(thisValue);
        }
        return aggregate;
      }

      @Override
      public List<String> merge(final List<String> aggOne, final List<String> aggTwo) {
        for (String thisEntry : aggTwo) {
          if (!aggOne.contains(thisEntry)) {
            aggOne.add(thisEntry);
          }
        }
        return aggOne;
      }
    };
  }

  @UdafFactory(description = "collect distinct values of a Boolean field into a single Array")
  public static Udaf<Boolean, List<Boolean>> createCollectSetBool() {
    return new Udaf<Boolean, List<Boolean>>() {
      @Override
      public List<Boolean> initialize() {
        return Lists.newArrayList();
      }

      @Override
      public List<Boolean> aggregate(final Boolean thisValue, final List<Boolean> aggregate) {
        if (!aggregate.contains(thisValue)) {
          aggregate.add(thisValue);
        }
        return aggregate;
      }

      @Override
      public List<Boolean> merge(final List<Boolean> aggOne, final List<Boolean> aggTwo) {
        for (Boolean thisEntry : aggTwo) {
          if (!aggOne.contains(thisEntry)) {
            aggOne.add(thisEntry);
          }
        }
        return aggOne;
      }
    };
  }

}
