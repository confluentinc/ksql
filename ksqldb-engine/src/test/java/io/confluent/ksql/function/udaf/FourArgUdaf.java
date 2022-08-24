/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.Quadruple;
import java.util.Arrays;

@UdafDescription(
        name = "FOUR_ARG",
        description = "Returns the sum of the provided longs and lengths of strings.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
@SuppressWarnings("unused")
public class FourArgUdaf implements Udaf<Quadruple<Long, String, String, String>, Long, Long> {
  @UdafFactory(description = "Testing factory")
  public static Udaf<Quadruple<Long, String, String, String>, Long, Long> createMultiArgUdaf(
          final int initArg1, final String... initArg2) {
    return new FourArgUdaf(initArg1, initArg2);
  }

  private final long initVal;

  public FourArgUdaf(final int initArg1, final String... initArg2) {
    initVal = initArg1 + Arrays.stream(initArg2).map(String::length).reduce(0, Integer::sum);
  }

  @Override
  public Long initialize() {
    return initVal;
  }

  @Override
  public Long aggregate(final Quadruple<Long, String, String, String> currentValue,
                        final Long aggregateValue) {
    final long firstVal = currentValue.getFirst() == null ? 0 : currentValue.getFirst();
    final int secondVal = currentValue.getSecond() == null ? 0 : currentValue.getSecond().length();
    final int thirdVal = currentValue.getThird() == null ? 0 : currentValue.getThird().length();
    final int fourthVal = currentValue.getFourth() == null ? 0 : currentValue.getFourth().length();

    return aggregateValue + firstVal + secondVal + thirdVal + fourthVal;
  }

  @Override
  public Long merge(final Long aggOne, final Long aggTwo) {
    return aggOne + aggTwo;
  }

  @Override
  public Long map(final Long agg) {
    return agg;
  }
}
