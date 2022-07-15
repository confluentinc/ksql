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

package io.confluent.ksql.function.udaf.correlation;

import io.confluent.ksql.function.udaf.Udaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.function.udaf.VariadicArgs;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.Pair;

@UdafDescription(
        name = "CORRELATION",
        description = "Testing multiple and variadic arguments",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public class CorrelationKudaf
        implements Udaf<Pair<Double, VariadicArgs<Double>>, Double, Double> {

  @UdafFactory(description = "Testing factory")
  public static Udaf<Pair<Double, VariadicArgs<Double>>, Double, Double> createCorrelation() {
    return new CorrelationKudaf();
  }

  @Override
  public Double initialize() {
    return 0.0;
  }

  @Override
  public Double aggregate(final Pair<Double, VariadicArgs<Double>> currentValue,
                          final Double aggregateValue) {
    return aggregateValue + currentValue.getLeft() + currentValue.getRight().get(0);
  }

  @Override
  public Double merge(Double aggOne, Double aggTwo) {
    return aggOne + aggTwo;
  }

  @Override
  public Double map(Double agg) {
    return agg;
  }
}
