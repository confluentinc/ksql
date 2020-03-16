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

package io.confluent.ksql.function.udaf.min;

import io.confluent.ksql.function.udaf.BaseNumberKudaf;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.math.BigDecimal;

public class DecimalMinKudaf extends BaseNumberKudaf<BigDecimal> {

  DecimalMinKudaf(
      final String functionName,
      final Integer argIndexInValue,
      final SqlType returnSchema
  ) {
    super(functionName,
          argIndexInValue,
          returnSchema,
          BigDecimal::min,
          "Computes the minimum decimal value for a key.");
  }

}
