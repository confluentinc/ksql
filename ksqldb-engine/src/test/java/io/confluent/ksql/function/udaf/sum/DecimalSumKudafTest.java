/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import java.math.BigDecimal;
import java.util.Collections;

public class DecimalSumKudafTest extends BaseSumKudafTest<BigDecimal, DecimalSumKudaf> {

  protected TGenerator<BigDecimal> getNumberGenerator() {
    return BigDecimal::valueOf;
  }

  protected DecimalSumKudaf getSumKudaf() {
    DecimalSumKudaf udaf = (DecimalSumKudaf) SumKudaf.createSumDecimal();

    SqlArgument intDecimalArg = SqlArgument.of(SqlDecimal.of(19, 0));
    udaf.initializeTypeArguments(Collections.singletonList(intDecimalArg));

    return udaf;
  }

}
