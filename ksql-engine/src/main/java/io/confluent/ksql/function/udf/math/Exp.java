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

@UdfDescription(name = "exp", author = "Confluent",
        description = "Applies exponential function to a DOUBLE value.")
public class Exp {
  @Udf(description = "Returns Euler's number e raised to the power of a DOUBLE value.")
  public double exp(
      @UdfParameter(value = "a", description = "the exponent to raise e to.") final double a) {
    try {
      return Math.exp(a);
    } catch (final ExecutionException | RuntimeException e) {
      throw new KsqlFunctionException("Failed to apply exponential on " + a
                    + ": " + e.getMessage(), e);
    }
  }
}