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

package io.confluent.ksql.function.udf.map;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Transform a map's key and values using two lambda functions
 */
@UdfDescription(
    name = "map_transform",
    category = FunctionCategory.MAP,
    description = "Apply one function to each key and " 
        + "one function to each value of a map. " 
        + "The two arguments for each function are in order: key, value. " 
        + "The first function provided will be applied to each key and the " 
        + "second one applied to each value. "
        + "The transformed map is returned.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class MapTransform {

  @Udf
  public <K,V,R,T> Map<R,T> mapTransform(
      @UdfParameter(description = "The map") final Map<K, V> map,
      @UdfParameter(description = "The key lambda function") final BiFunction<K, V, R> biFunction1,
      @UdfParameter(description = "The value lambda function") final BiFunction<K, V, T> biFunction2
  ) {
    if (map == null) {
      return null;
    }

    return map.entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> biFunction1.apply(entry.getKey(), entry.getValue()),
            entry -> biFunction2.apply(entry.getKey(), entry.getValue())));
  }
}
