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
        name = "power",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "Given a base and an exponent, returns the result of the base raised to the "
                + "exponent."
)
public class Power {

  @Udf(description = "Returns the INT base raised to the INT exponent.")
  public Double power(
          @UdfParameter(
                  value = "base",
                  description = "the base of the power."
          ) final Integer base,
          @UdfParameter(
                  value = "exponent",
                  description = "the exponent of the power."
          ) final Integer exponent
  ) {
    return power(
            base == null ? null : base.doubleValue(),
            exponent == null ? null : exponent.doubleValue()
    );
  }

  @Udf(description = "Returns the BIGINT base raised to the BIGINT exponent.")
  public Double power(
          @UdfParameter(
                  value = "base",
                  description = "the base of the power."
          ) final Long base,
          @UdfParameter(
                  value = "exponent",
                  description = "the exponent of the power."
          ) final Long exponent
  ) {
    return power(
            base == null ? null : base.doubleValue(),
            exponent == null ? null : exponent.doubleValue()
    );
  }

  @Udf(description = "Returns the DOUBLE base raised to the DOUBLE exponent.")
  public Double power(
          @UdfParameter(
                  value = "base",
                  description = "the base of the power."
          ) final Double base,
          @UdfParameter(
                  value = "exponent",
                  description = "the exponent of the power."
          ) final Double exponent
  ) {
    return base == null || exponent == null ? null : Math.pow(base, exponent);
  }
}
