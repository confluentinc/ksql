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

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@SuppressWarnings("WeakerAccess") // Invoked via reflection
@UdfDescription(
    name = "sign",
    category = FunctionCategory.MATHEMATICAL,
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "The sign of a value."
)
public class Sign {
  @Udf(description = "Returns the sign of an INT value, denoted by 1, 0 or -1.")
  public Integer sign(
      @UdfParameter(
          value = "value",
          description = "The value to get the sign of."
      ) final Integer value
  ) {
    return value == null
        ? null
        : Integer.signum(value);
  }

  @Udf(description = "Returns the sign of an BIGINT value, denoted by 1, 0 or -1.")
  public Integer sign(
      @UdfParameter(
          value = "value",
          description = "The value to get the sign of."
      ) final Long value
  ) {
    return value == null
        ? null
        : Long.signum(value);
  }

  @Udf(description = "Returns the sign of an DOUBLE value, denoted by 1, 0 or -1.")
  public Integer sign(
      @UdfParameter(
          value = "value",
          description = "The value to get the sign of."
      ) final Double value
  ) {
    return value == null
        ? null
        : (int) Math.signum(value);
  }
}
