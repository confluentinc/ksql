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

@UdfDescription(name = RoundKudf.NAME, version = "Confluent",
    description = "Round a value to the nearest BIGINT value.")
public class RoundKudf {
  static final String NAME = "ROUND";

  @Udf(description = "Round a value to the nearest BIGINT value.")
  public Long round(
      @UdfParameter(description = "A number which will be rounded up.")
      final Integer n) {
    if (n == null) {
      return null;
    }

    return Math.round(n.doubleValue());
  }

  @Udf(description = "Round a value to the nearest BIGINT value.")
  public Long round(
      @UdfParameter(description = "A number which will be rounded up.")
      final Long n) {
    if (n == null) {
      return null;
    }

    return Math.round(n.doubleValue());
  }

  @Udf(description = "Round a value to the nearest BIGINT value.")
  public Long round(
      @UdfParameter(description = "A number which will be rounded up.")
      final Double n) {
    if (n == null) {
      return null;
    }

    return Math.round(n);
  }
}
