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
import io.confluent.ksql.util.Quintuple;
import java.util.Arrays;

@UdafDescription(
        name = "FIVE_ARG",
        description = "Returns the sum of the provided longs, lengths of strings, and integers.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
@SuppressWarnings("unused")
public class FiveArgUdaf
        implements Udaf<Quintuple<Long, String, String, String, Integer>, Long, Long> {

  @UdafFactory(description = "Testing factory")
  public static Udaf<Quintuple<Long, String, String, String, Integer>, Long, Long>
      createMultiArgUdaf(final int initArg1, final String... initArg2) {
    return new FiveArgUdaf(initArg1, initArg2);
  }

  private final long initVal;

  public FiveArgUdaf(final int initArg1, final String... initArg2) {
    initVal = initArg1 + Arrays.stream(initArg2).map(String::length).reduce(0, Integer::sum);
  }

  @Override
  public Long initialize() {
    return initVal;
  }

  @Override
  public Long aggregate(final Quintuple<Long, String, String, String, Integer> currentValue,
                        final Long aggregateValue) {
    final long firstVal = currentValue.getFirst() == null ? 0 : currentValue.getFirst();
    final int secondVal = currentValue.getSecond() == null ? 0 : currentValue.getSecond().length();
    final int thirdVal = currentValue.getThird() == null ? 0 : currentValue.getThird().length();
    final int fourthVal = currentValue.getFourth() == null ? 0 : currentValue.getFourth().length();
    final int fifthVal = currentValue.getFifth() == null ? 0 : currentValue.getFifth();

    return aggregateValue + firstVal + secondVal + thirdVal + fourthVal + fifthVal;
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
