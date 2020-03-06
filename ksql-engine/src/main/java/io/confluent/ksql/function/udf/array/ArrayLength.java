/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udf.array;

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.List;

/**
 * Returns the length of an array
 */
@SuppressWarnings("MethodMayBeStatic") // UDF methods can not be static.
@UdfDescription(name = "ARRAY_LENGTH", description = "Returns the length on an array")
public class ArrayLength {

  @Udf
  public <T> int calcArrayLength(
      @UdfParameter(description = "The array") final List<T> array
  ) {
    return array == null
        ? 0
        : array.size();
  }
}