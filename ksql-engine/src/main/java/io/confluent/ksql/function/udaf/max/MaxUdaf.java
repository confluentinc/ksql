/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function.udaf.max;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.function.BiFunction;

@UdafDescription(name = "max",
    description = "Returns the maximum value for a given column.")

public final class MaxUdaf {

  private MaxUdaf() {
  }


  @UdafFactory(description = "Returns the maximum long value.")
  public static Udaf<Long, Long, Long> maxLong() {

    return getMaxImplementation(Long::max);
  }

  @UdafFactory(description = "Returns the maximum int value.")
  public static Udaf<Integer, Integer, Integer> maxInt() {

    return getMaxImplementation(Integer::max);
  }

  @UdafFactory(description = "Returns the maximum double value.")
  public static Udaf<Double, Double, Double> maxDouble() {

    return getMaxImplementation(Double::max);
  }

  private static  <I> Udaf<I, I, I> getMaxImplementation(final BiFunction<I, I, I> maxer) {

    return new Udaf<I, I, I>() {

      @Override
      public I initialize() {
        return null;
      }

      @Override
      public I aggregate(final I newValue, final I aggregateValue) {
        if (newValue == null) {
          return aggregateValue;
        }
        if (aggregateValue == null) {
          return newValue;
        }

        return maxer.apply(newValue, aggregateValue);
      }

      @Override
      public I map(final I aggregate) {
        return aggregate;
      }

      @Override
      public I merge(final I agg1, final I agg2) {
        if (agg1 == null) {
          return agg2;
        }
        if (agg2 == null) {
          return agg1;
        }
        return maxer.apply(agg1, agg2);
      }
    };
  }
}
