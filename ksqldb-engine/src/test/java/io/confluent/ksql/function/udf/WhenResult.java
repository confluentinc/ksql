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

package io.confluent.ksql.function.udf;

import io.confluent.ksql.util.KsqlException;

@SuppressWarnings({"unused", "MethodMayBeStatic"}) // Invoked via reflection
@UdfDescription(name="WhenResult", description = "UDF used in case-expression.json")
public class WhenResult {

  @Udf
  public int evaluate(final int retVal, final boolean shouldBeEvaluated) {
    if (!shouldBeEvaluated) {
      throw new KsqlException("Then expression in case is not running lazily!");
    }
    return retVal;
  }
}