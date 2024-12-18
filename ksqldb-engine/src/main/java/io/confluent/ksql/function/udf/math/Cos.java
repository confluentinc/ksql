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
        name = "cos",
        category = FunctionCategory.MATHEMATICAL,
        author = KsqlConstants.CONFLUENT_AUTHOR,
        description = "The cosine of a value."
)
public class Cos {

  @Udf(description = "Returns the cosine of an INT value")
  public Double cos(
          @UdfParameter(
                  value = "value",
                  description = "The value in radians to get the cosine of."
          ) final Integer value
  ) {
    return cos(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the cosine of a BIGINT value")
  public Double cos(
          @UdfParameter(
                  value = "value",
                  description = "The value in radians to get the cosine of."
          ) final Long value
  ) {
    return cos(value == null ? null : value.doubleValue());
  }

  @Udf(description = "Returns the cosine of a DOUBLE value")
  public Double cos(
          @UdfParameter(
                  value = "value",
                  description = "The value in radians to get the cosine of."
          ) final Double value
  ) {
    return value == null
            ? null
            : Math.cos(value);
  }
}
