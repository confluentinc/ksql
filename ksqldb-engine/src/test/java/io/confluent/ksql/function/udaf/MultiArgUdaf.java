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
import io.confluent.ksql.util.Pair;

@UdafDescription(
        name = "MULTI_ARG",
        description = "Testing multiple arguments",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class MultiArgUdaf implements Udaf<Pair<Long, String>, Double, Double> {

  @UdafFactory(description = "Testing factory")
  @SuppressWarnings("unused")
  public static Udaf<Pair<Long, String>, Double, Double> createMultiArgUdaf(final int initArg) {
    return new MultiArgUdaf();
  }

  @Override
  public Double initialize() {
    return 0.0;
  }

  @Override
  public Double aggregate(final Pair<Long, String> currentValue,
                          final Double aggregateValue) {
    return aggregateValue + currentValue.getLeft() + currentValue.getRight().length();
  }

  @Override
  public Double merge(final Double aggOne, final Double aggTwo) {
    return aggOne + aggTwo;
  }

  @Override
  public Double map(final Double agg) {
    return agg;
  }
}
