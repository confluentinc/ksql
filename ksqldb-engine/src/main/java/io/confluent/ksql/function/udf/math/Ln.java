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

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;

@UdfDescription(
    name = "ln",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "The natural logarithm of a value."
)
public class Ln {

  @Udf(description = "Returns the natural logarithm (base e) of an INT value.")
  public Double ln(
        @UdfParameter(
            value = "value",
            description = "the value get the natual logarithm of."
        ) final Integer value
  ) {
    return ln(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the natural logarithm (base e) of a BIGINT value.")
  public Double ln(
      @UdfParameter(
          value = "value",
          description = "the value get the natual logarithm of."
      ) final Long value
  ) {
    return ln(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the natural logarithm (base e) of a DOUBLE value.")
  public Double ln(
      @UdfParameter(
          value = "value",
          description = "the value get the natual logarithm of."
      ) final Double value
  ) {
    return value == null
        ? null
        : Math.log(value);
  }
}
