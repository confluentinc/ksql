/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function.udtf.array;

import io.confluent.ksql.function.udtf.Udtf;
import io.confluent.ksql.function.udtf.UdtfDescription;
import io.confluent.ksql.util.KsqlConstants;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of the 'explode' table function. This table function takes an array of values and
 * explodes it into zero or more rows, one for each value in the array.
 */
@UdtfDescription(name = "explode", author = KsqlConstants.CONFLUENT_AUTHOR,
    description =
        "Explodes an array. This function outputs one value for each element of the array.")
public class Explode {

  @Udtf
  public List<Long> explodeLong(final List<Long> input) {
    return explode(input);
  }

  @Udtf
  public List<Integer> explodeInt(final List<Integer> input) {
    return explode(input);
  }

  @Udtf
  public List<Double> explodeDouble(final List<Double> input) {
    return explode(input);
  }

  @Udtf
  public List<Boolean> explodeBoolean(final List<Boolean> input) {
    return explode(input);
  }

  @Udtf
  public List<String> explodeString(final List<String> input) {
    return explode(input);
  }

  @Udtf
  public List<BigDecimal> explodeBigDecimal(final List<BigDecimal> input) {
    return explode(input);
  }

  private <T> List<T> explode(final List<T> list) {
    return list == null ? Collections.emptyList() : list;
  }

}
