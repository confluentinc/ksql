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

@UdfDescription(name = AbsKudf.NAME, version = "Confluent",
    description = "Returns the absolute value of a number.")
public class AbsKudf {
  public static final String NAME = "ABS";

  @Udf(description = "Returns the absolute value of a number.")
  public Integer abs(
      @UdfParameter(description = "A number whose absolute value is to be retrieved.")
      final Integer n) {
    if (n == null) {
      return null;
    }

    return Math.abs(n);
  }

  @Udf(description = "Returns the absolute value of a number.")
  public Long abs(
      @UdfParameter(description = "A number whose absolute value is to be retrieved.")
      final Long n) {
    if (n == null) {
      return null;
    }

    return Math.abs(n);
  }

  @Udf(description = "Returns the absolute value of a number.")
  public Double abs(
      @UdfParameter(description = "A number whose absolute value is to be retrieved.")
      final Double n) {
    if (n == null) {
      return null;
    }

    return Math.abs(n);
  }
}
