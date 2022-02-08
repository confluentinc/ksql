/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.function.udf.lambda;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Filter a collection through a function, returning a subset of the collection
 */
@UdfDescription(
    name = "filter",
    category = FunctionCategory.LAMBDA,
    description = "Filter the input collection through a given "
        + "lambda function. "
        + "The filtered collection is returned.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Filter {
  @Udf(description = "When filtering an array, "
      + "the function provided must have a boolean result. "
      + "The function is applied to each value in the array "
      + "and a filtered array is returned."
  )
  public <T> List<T> filterArray(
      @UdfParameter(description = "The array") final List<T> array,
      @UdfParameter(description = "The lambda function") final Function<T, Boolean> function
  ) {
    if (array == null || function == null) {
      return null;
    }
    return array.stream().filter(function::apply).collect(Collectors.toList());
  }

  @Udf(description = "When filtering a map, "
      + "the function provided must have a boolean result. "
      + "For each map entry, the function will be applied to the "
      + "key and value arguments in that order. The filtered map is returned."
  )
  public <K, V> Map<K, V> filterMap(
      @UdfParameter(description = "The map") final Map<K, V> map,
      @UdfParameter(description = "The lambda function") final BiFunction<K, V, Boolean> biFunction
  ) {
    if (map == null || biFunction == null) {
      return null;
    }

    return map.entrySet()
        .stream()
        .filter(e -> biFunction.apply(e.getKey(), e.getValue()))
        .collect(Collectors.toMap(Entry::getKey, Entry::getValue));
  }
}
