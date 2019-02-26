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

@UdfDescription(name = CeilKudf.NAME, version = "Confluent",
    description = "Returns the ceiling of a value.")
public class CeilKudf {
  static final String NAME = "CEIL";

  @Udf(description = "Returns the ceiling of a value.")
  public Integer ceil(@UdfParameter(description = "A value.")
      final Integer n) {
    if (n == null) {
      return null;
    }

    return (int) Math.ceil(n.doubleValue());
  }

  @Udf(description = "Returns the ceiling of a value.")
  public Long ceil(@UdfParameter(description = "A value.")
                      final Long n) {
    if (n == null) {
      return null;
    }

    return (long) Math.ceil(n.doubleValue());
  }

  @Udf(description = "Returns the ceiling of a value.")
  public Double ceil(@UdfParameter(description = "A value.")
                      final Double n) {
    if (n == null) {
      return null;
    }

    return Math.ceil(n.doubleValue());
  }
}
