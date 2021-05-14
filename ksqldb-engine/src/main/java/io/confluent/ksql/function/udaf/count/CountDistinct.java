/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.count;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.clearspring.analytics.stream.cardinality.RegisterSet;
import com.google.common.primitives.Ints;
import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;

@UdafDescription(
    name = "COUNT_DISTINCT",
    description = CountDistinct.DESCRIPTION,
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public final class CountDistinct {

  static final String DESCRIPTION = "This function returns the number of items found in a group. "
      + "The implementation is probabilistic with a typical accuracy (standard error) of less "
      + "than 1%.";

  // magic number causes accuracy < .01 - we can consider making
  // this configurable if the need arises
  private static final int M = 1 << 14;
  private static final int LOG_2_M = 14;

  private CountDistinct() {
  }

  // NOTE: since our UDAF framework requires the aggregate values to
  // be serializable, and we don't support serialization of native int[],
  // this implementation can be optimized by avoiding conversions between
  // int[] and List<Integer> - since RegisterSet requires an int[], we would
  // need to duplicate a lot of code to get this to be zero-copy
  private static <T> Udaf<T, List<Integer>, Long> countDistinct() {
    return new Udaf<T, List<Integer>, Long>() {

      @Override
      public List<Integer> initialize() {
        return Ints.asList(new int[RegisterSet.getSizeForCount(M)]);
      }

      @Override
      public List<Integer> aggregate(final T current, final List<Integer> aggregate) {
        if (current == null) {
          return aggregate;
        }

        // this operation updates the underlying bytes
        final int[] ints = Ints.toArray(aggregate);
        final RegisterSet set = new RegisterSet(M, ints);

        // this modifies the underlying ints
        toHyperLogLog(set).offer(current);

        return Ints.asList(ints);
      }

      @Override
      public List<Integer> merge(final List<Integer> aggOne, final List<Integer> aggTwo) {
        final RegisterSet registerSet = new RegisterSet(M, Ints.toArray(aggOne));
        registerSet.merge(new RegisterSet(M, Ints.toArray(aggTwo)));

        return Ints.asList(registerSet.bits());
      }

      @Override
      public Long map(final List<Integer> agg) {
        return toHyperLogLog(new RegisterSet(M, Ints.toArray(agg))).cardinality();
      }
    };
  }

  @SuppressWarnings("deprecation")
  private static HyperLogLog toHyperLogLog(final RegisterSet set) {
    return new HyperLogLog(LOG_2_M, set);
  }

  @UdafFactory(description = "Count distinct")
  public static <T> Udaf<T, List<Integer>, Long> distinct() {
    return countDistinct();
  }

}
