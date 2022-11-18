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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Transform a collection with a function
 */
@UdfDescription(
    name = "transform",
    category = FunctionCategory.LAMBDA,
    description = "Apply a function to each element in a collection. " 
        + "The transformed collection is returned.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Transform {

  @Udf(description = "When transforming an array, "
      + "the function provided must have two arguments. "
      + "The two arguments for each function are in order: " 
      + "the key and then the value. "
      + "The transformed array is returned."
  )
  public <T, R> List<R> transformArray(
      @UdfParameter(description = "The array") final List<T> array,
      @UdfParameter(description = "The lambda function") final Function<T, R> function
  ) {
    if (array == null || function == null) {
      return null;
    }
    return array.stream().map(function).collect(Collectors.toList());
  }

  @Udf(description = "When transforming a map, "
      + "two functions must be provided. "
      + "For each map entry, the first function provided will "
      + "be applied to the key and the second one applied to the value. "
      + "Each function must have two arguments. "
      + "The two arguments for each function are in order: the key and then the value. "
      + "The transformed map is returned."
  )
  public <K,V,R,T> Map<R,T> transformMap(
      @UdfParameter(description = "The map") final Map<K, V> map,
      @UdfParameter(description = "The key lambda function") final BiFunction<K, V, R> biFunction1,
      @UdfParameter(description = "The value lambda function") final BiFunction<K, V, T> biFunction2
  ) {
    if (map == null || biFunction1 == null || biFunction2 == null) {
      return null;
    }

    return map.entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> biFunction1.apply(entry.getKey(), entry.getValue()),
            entry -> biFunction2.apply(entry.getKey(), entry.getValue())));
  }
}
