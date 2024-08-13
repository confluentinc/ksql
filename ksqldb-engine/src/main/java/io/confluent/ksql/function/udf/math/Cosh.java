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

package io.confluent.ksql.function.udf.math;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
        name = "cosh",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The hyperbolic cosine of a value."
)
public class Cosh {

  @Udf(description = "Returns the hyperbolic cosine of an INT value")
  public Double cosh(
          @UdfParameter(
                  value = "value",
                  description = "The value in radians to get the hyperbolic cosine of."
          ) final Integer value
  ) {
    return cosh(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the hyperbolic cosine of a BIGINT value")
  public Double cosh(
          @UdfParameter(
                  value = "value",
                  description = "The value in radians to get the hyperbolic cosine of."
          ) final Long value
  ) {
    return cosh(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the hyperbolic cosine of a DOUBLE value")
  public Double cosh(
          @UdfParameter(
                  value = "value",
                  description = "The value in radians to get the hyperbolic cosine of."
          ) final Double value
  ) {
    return value == null
            ? null
            : Math.cosh(value);
  }
}
