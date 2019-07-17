/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
    name = "log",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "The natural logarithm of a value."
)
public class Log {

  @Udf(description = "Returns the natural logarithm (base e) of an INT value.")
  public Double log(
        @UdfParameter(
            value = "value",
            description = "the value get the natual logarithm of."
        ) final Integer value
  ) {
    return log(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the natural logarithm (base e) of a BIGINT value.")
  public Double log(
      @UdfParameter(
          value = "value",
          description = "the value get the natual logarithm of."
      ) final Long value
  ) {
    return log(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the natural logarithm (base e) of a DOUBLE value.")
  public Double log(
      @UdfParameter(
          value = "value",
          description = "the value get the natual logarithm of."
      ) final Double value
  ) {
    if (value == null) {
      return null;
    }

    final double result = Math.log(value);
    if (Double.isNaN(result)) {
      throw new KsqlFunctionException("Result was NaN");
    }
    if (Double.isInfinite(result)) {
      throw new KsqlFunctionException("Result was infinite");
    }

    return result;
  }
}
