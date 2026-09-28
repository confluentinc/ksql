/*
 * Copyright 2023 Confluent Inc.
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

package io.confluent.ksql.function.udf.conversions;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;

@UdfDescription(
    name = "int_from_boolean",
    category = FunctionCategory.CONVERSIONS,
    description = "Converts a Boolean value to an Integer value"
)
public class IntFromBoolean {

  @Udf(description = "Converts a Boolean value to an Integer value")
  public Integer intFromBoolean(
      @UdfParameter(description = "The Boolean value to convert.")
      final Boolean bool
  ) {
    if (bool == null) {
      return null;
    }
    return Boolean.TRUE.equals(bool) ? 1 : 0;
  }
}
