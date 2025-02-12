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

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.udaf.TableUdaf;
import io.confluent.ksql.function.udaf.UdafDescription;
import io.confluent.ksql.function.udaf.UdafFactory;
import io.confluent.ksql.util.KsqlConstants;
import java.math.BigDecimal;

@UdafDescription(
        name = "SUM",
        description = "Computes the sum for a key.",
        author = KsqlConstants.CONFLUENT_AUTHOR
)
public abstract class SumKudaf<T> implements TableUdaf<T, T, T> {
  public static final String DESCRIPTION = "Computes the sum for a key.";

  @UdafFactory(description = "Computes the sum of decimal values for a key, "
          + "resulting in a decimal with the same precision and scale.")
  public static TableUdaf<BigDecimal, BigDecimal, BigDecimal> createSumDecimal() {
    return new DecimalSumKudaf();
  }

  @UdafFactory(description = DESCRIPTION)
  public static TableUdaf<Double, Double, Double> createSumDouble() {
    return new DoubleSumKudaf();
  }

  @UdafFactory(description = DESCRIPTION)
  public static TableUdaf<Long, Long, Long> createSumLong() {
    return new LongSumKudaf();
  }

  @UdafFactory(description = DESCRIPTION)
  public static TableUdaf<Integer, Integer, Integer> createSumInt() {
    return new IntegerSumKudaf();
  }
}
