/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.array;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@UdfDescription(
    name = "array_remove",
    category = FunctionCategory.ARRAY,
    description = "Removes all the elements equal to a specific value from an array."
        + " Returns NULL if the input array is NULL.")
public class ArrayRemove {

  @Udf
  public <T> List<T> remove(
      @UdfParameter(description = "Array of values") final List<T> array,
      @UdfParameter(description = "Value to remove") final T victim) {
    if (array == null) {
      return null;
    }

    return array.stream()
        .filter(el -> !Objects.equals(el, victim))
        .collect(Collectors.toList());
  }
}