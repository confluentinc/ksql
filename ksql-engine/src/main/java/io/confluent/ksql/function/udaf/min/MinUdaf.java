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

package io.confluent.ksql.function.udaf.min;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import java.util.function.BiFunction;

@UdafDescription(name = "min",
    description = "Returns the minimum value for a given column.")

public final class MinUdaf {

  private MinUdaf() {
  }


  @UdafFactory(description = "Returns the minimum long value.")
  public static Udaf<Long, Long, Long> minLong() {

    return getMinImplementation(Long::min);
  }

  @UdafFactory(description = "Returns the minimum int value.")
  public static Udaf<Integer, Integer, Integer> minInt() {

    return getMinImplementation(Integer::min);
  }

  @UdafFactory(description = "Returns the minimum double value.")
  public static Udaf<Double, Double, Double> minDouble() {

    return getMinImplementation(Double::min);
  }

  private static <I> Udaf<I, I, I> getMinImplementation(final BiFunction<I, I, I> miner) {

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
        return miner.apply(newValue, aggregateValue);
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
        return miner.apply(agg1, agg2);
      }
    };
  }
}
