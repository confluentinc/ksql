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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@UdfDescription(
    name = "array_distinct",
    category = FunctionCategory.ARRAY,
    description = "Returns an array of all the distinct values, including NULL if present, from"
        + " the input array."
        + " The output array elements will be in order of their first occurrence in the input."
        + " Returns NULL if the input array is NULL.")
public class ArrayDistinct {

  @Udf
  public <T> List<T> distinct(
      @UdfParameter(description = "Array of values to distinct") final List<T> input) {
    if (input == null) {
      return null;
    }
    final Set<T> distinctVals = Sets.newLinkedHashSetWithExpectedSize(input.size());
    distinctVals.addAll(input);
    return new ArrayList<>(distinctVals);
  }
}