/*
 * Copyright 2022 Confluent Inc.
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

package io.confluent.ksql.function.udaf.min;

import io.confluent.ksql.function.udaf.BaseComparableKudaf;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.utils.FormatOptions;

public class MinKudaf<T extends Comparable<? super T>> extends BaseComparableKudaf<T> {

  MinKudaf(
      final String functionName,
      final Integer argIndexInValue,
      final SqlType outputSchema
  ) {
    super(functionName,
        argIndexInValue,
        outputSchema,
        (first, second) -> first.compareTo(second) < 0 ? first : second,
        "Computes the minimum " + outputSchema.toString(FormatOptions.none())
            + " value for a key.");
  }

}
