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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.List;
import java.util.Set;

@UdfDescription(
    name = "array_intersect",
    category = FunctionCategory.ARRAY,
    description = "Returns an array of all the distinct elements from the intersection of both"
        + " input arrays, or NULL if either input array is NULL. The order of entries in the"
        + " output is the same as in the first input array.")
public class ArrayIntersect {

  @Udf
  public <T> List<T> intersect(
      @UdfParameter(description = "First array of values") final List<T> left,
      @UdfParameter(description = "Second array of values") final List<T> right) {
    if (left == null || right == null) {
      return null;
    }
    final Set<T> intersection = Sets.newLinkedHashSet(left);
    intersection.retainAll(Sets.newHashSet(right));
    return Lists.newArrayList(intersection);
  }

}