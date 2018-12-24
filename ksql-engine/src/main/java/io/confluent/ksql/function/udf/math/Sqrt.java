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

@UdfDescription(name = "sqrt", author = "Confluent",
    description = "Applies square root function to a DOUBLE value.")
public class Sqrt {
  @Udf(description = "Returns the correctly rounded positive square root of a DOUBLE value")
  public double sqrt(
    @UdfParameter(value = "a", description = "a value.") final double a) {
      try {
        return Math.sqrt(a);
      } catch (final ExecutionException | RuntimeException e) {
        throw new KsqlFunctionException("Failed to apply square root on " + a
          + ": " + e.getMessage(), e);
      }
    }
}
