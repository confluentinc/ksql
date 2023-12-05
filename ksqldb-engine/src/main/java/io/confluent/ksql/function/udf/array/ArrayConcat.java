/*
 * Copyright 2021 Confluent Inc.
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
import java.util.ArrayList;
import java.util.List;


@UdfDescription(
    name = "array_concat",
    category = FunctionCategory.ARRAY,
    description = "Concatenates two arrays, creating an array that contains all the elements"
        + " in the first array followed by all the elements in the second array."
        + " Returns NULL if both input arrays are NULL."
        + " If only one argument is NULL, the result is the other argument."
        + " The two arrays must be of the same type.")
public class ArrayConcat {
  @Udf
  public <T> List<T> concat(
      @UdfParameter(description = "First array of values") final List<T> left,
      @UdfParameter(description = "Second array of values") final List<T> right) {
    if (left == null && right == null) {
      return null;
    }
    final int leftSize = left != null ? left.size() : 0;
    final int rightSize = right != null ? right.size() : 0;
    final List<T> result = new ArrayList<>(leftSize + rightSize);
    if (left != null) {
      result.addAll(left);
    }
    if (right != null) {
      result.addAll(right);
    }
    return result;
  }
}
