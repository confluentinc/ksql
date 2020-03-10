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

@SuppressWarnings("WeakerAccess") // Invoked via reflection
@UdfDescription(
    name = "exp",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "The exponential of a value."
)
public class Exp {

  @Udf(description = "Returns Euler's number e raised to the power of an INT value.")
  public Double exp(
      @UdfParameter(
          value = "exponent",
          description = "the exponent to raise e to."
      ) final Integer exponent
  ) {
    return exp(exponent == null ? null : exponent.doubleValue());
  }

  @Udf(description = "Returns Euler's number e raised to the power of a BIGINT value.")
  public Double exp(
      @UdfParameter(
          value = "exponent",
          description = "the exponent to raise e to."
      ) final Long exponent
  ) {
    return exp(exponent == null ? null : exponent.doubleValue());
  }

  @Udf(description = "Returns Euler's number e raised to the power of a DOUBLE value.")
  public Double exp(
      @UdfParameter(
          value = "exponent",
          description = "the exponent to raise e to."
      ) final Double exponent
  ) {
    return exponent == null
        ? null
        : Math.exp(exponent);
  }
}