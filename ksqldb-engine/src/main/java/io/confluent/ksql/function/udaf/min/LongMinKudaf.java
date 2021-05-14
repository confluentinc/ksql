/*
 * Copyright 2021 Confluent Inc.
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

import io.confluent.ksql.function.udaf.BaseNumberKudaf;
import io.confluent.ksql.schema.ksql.types.SqlTypes;

public class LongMinKudaf extends BaseNumberKudaf<Long> {

  LongMinKudaf(final String functionName, final int argIndexInValue) {
    super(functionName,
          argIndexInValue,
          SqlTypes.BIGINT,
          Long::min,
          "Computes the minimum long value for a key.");
  }

}
