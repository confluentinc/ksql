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
import java.util.Objects;

@UdafDescription(
        name = "VAR_ARG",
        description = "Returns the sum of the provided longs and lengths of strings.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
@SuppressWarnings("unused")
public class VarArgUdaf implements Udaf<Pair<Long, VariadicArgs<String>>, Long, Long> {

  @UdafFactory(description = "Testing factory")
  public static Udaf<Pair<Long, VariadicArgs<String>>, Long, Long> createVarArgUdaf() {
    return new VarArgUdaf();
  }

  @Override
  public Long initialize() {
    return 0L;
  }

  @Override
  public Long aggregate(final Pair<Long, VariadicArgs<String>> currentValue,
                          final Long aggregateValue) {
    final long firstVal = currentValue.getLeft() == null ? 0 : currentValue.getLeft();
    final int secondVal = currentValue.getRight() == null ? 0 : currentValue.getRight().stream()
            .filter(Objects::nonNull)
            .map(String::length)
            .reduce(0, Integer::sum);

    return aggregateValue + firstVal + secondVal;
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