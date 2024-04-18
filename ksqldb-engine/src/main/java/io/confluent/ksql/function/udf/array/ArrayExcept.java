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
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@UdfDescription(
    name = "array_except",
    category = FunctionCategory.ARRAY,
    description = "Returns an array of all the elements in an array except for those also present"
        + " in a second array. The order of entries in the first array is preserved although any"
        + " duplicates are removed. Returns NULL if either input is NULL.")
public class ArrayExcept {

  @Udf
  public <T> List<T> except(
      @UdfParameter(description = "Array of values") final List<T> left,
      @UdfParameter(description = "Array of exceptions") final List<T> right) {
    if (left == null || right == null) {
      return null;
    }
    final Set<T> distinctRightValues = new HashSet<>(right);
    final Set<T> distinctLeftValues = new LinkedHashSet<>(left);
    return distinctLeftValues
        .stream()
        .filter(e -> !distinctRightValues.contains(e))
        .collect(Collectors.toList());
  }
}