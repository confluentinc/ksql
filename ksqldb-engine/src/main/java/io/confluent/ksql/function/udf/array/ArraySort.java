/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the
 * License.
 */

package io.confluent.ksql.function.udf.array;

import static java.util.Comparator.naturalOrder;
import static java.util.Comparator.nullsLast;

import com.google.common.collect.Lists;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.Collections;
import java.util.List;

/**
 * This UDF sorts the elements of an array according to their natural sort order.
 */
@UdfDescription(
    name = "array_sort",
    category = FunctionCategory.ARRAY,
    description = "Sort an array of primitive values, according to their natural sort order. Any "
        + "NULLs in the array will be placed at the end.")
public class ArraySort {

  private static final List<String> SORT_DIRECTION_ASC = Lists.newArrayList("ASC", "ASCENDING");
  private static final List<String> SORT_DIRECTION_DESC = Lists.newArrayList("DESC", "DESCENDING");

  @Udf
  public <T extends Comparable<? super T>> List<T> arraySortDefault(@UdfParameter(
      description = "The array to sort") final List<T> input) {
    return arraySortWithDirection(input, "ASC");
  }

  @Udf
  public <T extends Comparable<? super T>> List<T> arraySortWithDirection(@UdfParameter(
      description = "The array to sort") final List<T> input,
      @UdfParameter(
          description = "Marks the end of the series (inclusive)") final String direction) {
    if (input == null || direction == null) {
      return null;
    }
    if (SORT_DIRECTION_ASC.contains(direction.toUpperCase())) {
      input.sort(nullsLast(naturalOrder()));
    } else if (SORT_DIRECTION_DESC.contains(direction.toUpperCase())) {
      input.sort(nullsLast(Collections.reverseOrder()));
    } else {
      return null;
    }
    return input;
  }

}
