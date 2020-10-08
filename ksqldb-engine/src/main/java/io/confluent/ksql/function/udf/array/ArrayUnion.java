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

import com.google.common.collect.Sets;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

@UdfDescription(
    name = "array_union",
    category = FunctionCategory.ARRAY,
    description = "Returns an array of all the distinct elements from both input arrays, "
        + "or NULL if either array is NULL.")
public class ArrayUnion {

  @SuppressWarnings("unchecked")
  @Udf
  public <T> List<T> union(
      @UdfParameter(description = "First array of values") final List<T> left,
      @UdfParameter(description = "Second array of values") final List<T> right) {
    if (left == null || right == null) {
      return null;
    }
    final Set<T> combined = Sets.newLinkedHashSet(left);
    combined.addAll(right);
    return (List<T>) Arrays.asList(combined.toArray());
  }

}