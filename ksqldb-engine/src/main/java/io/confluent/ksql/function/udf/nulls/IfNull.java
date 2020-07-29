/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.function.udf.nulls;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@SuppressWarnings({"MethodMayBeStatic", "unused"}) // UDF methods can not be static.
@UdfDescription(
    name = "IFNULL",
    category = FunctionCategory.CONDITIONAL,
    description = "Returns expression if NOT NULL, otherwise the alternative value",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class IfNull {

  private static final Coalesce COALESCE = new Coalesce();

  @Udf
  public final <T> T ifNull(
      @UdfParameter(description = "expression to evaluate") final T expression,
      @UdfParameter(description = "Alternative value") final T altValue
  ) {
    return COALESCE.coalesce(expression, altValue);
  }
}
