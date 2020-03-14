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
    name = "sqrt",
    author = KsqlConstants.CONFLUENT_AUTHOR,
    description = "The square root of a value."
)
public class Sqrt {

  @Udf(description = "Returns the correctly rounded positive square root of a DOUBLE value")
  public Double sqrt(
      @UdfParameter(
          value = "value",
          description = "The value to get the square root of."
      ) final Integer value
  ) {
    return sqrt(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the correctly rounded positive square root of a DOUBLE value")
  public Double sqrt(
      @UdfParameter(
          value = "value",
          description = "The value to get the square root of."
      ) final Long value
  ) {
    return sqrt(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the correctly rounded positive square root of a DOUBLE value")
  public Double sqrt(
      @UdfParameter(
          value = "value",
          description = "The value to get the square root of."
      ) final Double value
  ) {
    return value == null
        ? null
        : Math.sqrt(value);
  }
}
