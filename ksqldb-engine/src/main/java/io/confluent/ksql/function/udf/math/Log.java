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
        name = "log",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The logarithm of a value."
)
public class Log {

  @Udf(description = "Returns the base 10 logarithm of an INT value.")
  public Double log(
          @UdfParameter(
                  value = "value",
                  description = "the value get the base 10 logarithm of."
          ) final Integer value
  ) {
    return log(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the base 10 logarithm of a BIGINT value.")
  public Double log(
          @UdfParameter(
                  value = "value",
                  description = "the value get the base 10 logarithm of."
          ) final Long value
  ) {
    return log(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the base 10 logarithm of a DOUBLE value.")
  public Double log(
          @UdfParameter(
                  value = "value",
                  description = "the value get the base 10 logarithm of."
          ) final Double value
  ) {
    return value == null
            ? null
            : Math.log(value);
  }

  @Udf(description = "Returns the logarithm with the given base of an INT value.")
  public Double log(
          @UdfParameter(
                  value = "base",
                  description = "the base of the logarithm."
          ) final Integer base,
          @UdfParameter(
                  value = "value",
                  description = "the value get the logarithm of."
          ) final Integer value
  ) {
    return log(
            base == null ? null : base.doubleValue(),
            value == null ? null : value.doubleValue()
    );
  }

  @Udf(description = "Returns the logarithm with the given base of a BIGINT value.")
  public Double log(
          @UdfParameter(
                  value = "base",
                  description = "the base of the logarithm."
          ) final Long base,
          @UdfParameter(
                  value = "value",
                  description = "the value get the logarithm of."
          ) final Long value
  ) {
    return log(
            base == null ? null : base.doubleValue(),
            value == null ? null : value.doubleValue()
    );
  }

  @Udf(description = "Returns the logarithm with the given base of a DOUBLE value.")
  public Double log(
          @UdfParameter(
                  value = "base",
                  description = "the base of the logarithm."
          ) final Double base,
          @UdfParameter(
                  value = "value",
                  description = "the value get the logarithm of."
          ) final Double value
  ) {

    if (base == null || value == null) {
      return null;
    }

    if (base <= 0 || base == 1) {
      return Double.NaN;
    }

    return Math.log(value) / Math.log(base);
  }
}