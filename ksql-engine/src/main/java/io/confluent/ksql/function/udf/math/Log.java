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

@UdfDescription(name = "log", author = "Confluent",
    description = "Applies natural logarithm function to a DOUBLE value.")
public class Log {
  @Udf(description = "Returns the natural logarithm (base e) of a DOUBLE value.")
  public double log(
    @UdfParameter(value = "a", description = "a value.") final double a) {
      try {
        return Math.log(a);
      } catch (final ExecutionException | RuntimeException e) {
        throw new KsqlFunctionException("Failed to apply logarithm on " + a
          + ": " + e.getMessage(), e);
      }
    }
}
