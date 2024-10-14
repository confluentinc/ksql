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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.List;

/**
 * This UDF traverses the elements of an Array field to find and return the minimum contained value.
 */
@UdfDescription(
    name = "array_min",
    category = FunctionCategory.ARRAY,
    description = "Return the minimum value from within an array of primitive values, according to"
        + " their natural sort order. If the array is NULL, or contains only NULLs, return NULL.")
public class ArrayMin {

  @Udf
  public <T extends Comparable<? super T>> T arrayMin(@UdfParameter(
      description = "Array of values from which to find the minimum") final List<T> input) {
    if (input == null) {
      return null;
    }

    T candidate = null;
    for (T thisVal : input) {
      if (thisVal != null) {
        if (candidate == null) {
          candidate = thisVal;
        } else if (thisVal.compareTo(candidate) < 0) {
          candidate = thisVal;
        }
      }
    }
    return candidate;
  }
}
