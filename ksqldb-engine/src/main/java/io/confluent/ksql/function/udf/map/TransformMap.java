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
import io.confluent.ksql.types.KsqlLambda;
import io.confluent.ksql.types.KsqlLambdaV2;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Transform a map's key and values using lambda functions
 */
@SuppressWarnings("MethodMayBeStatic") // UDF methods can not be static.
@UdfDescription(
    name = "TransformMap",
    category = FunctionCategory.MAP,
    description = "Apply a lambda function to both key and value of a map",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class TransformMap {

  @Udf
  public <K,V,R,T> Map<R,T> transformMap(
      @UdfParameter(description = "The map") final Map<K, V> map,
      @UdfParameter(description = "The new key lambda") final KsqlLambdaV2<K, V, R> lambda1,
      @UdfParameter(description = "The new value lambda") final KsqlLambdaV2<K, V, T> lambda2
  ) {
    if (map == null) {
      return null;
    }
    return map.entrySet()
        .stream()
        .collect(Collectors.toMap(
            entry -> lambda1.getFunction().apply(entry.getKey(), entry.getValue()),
            entry -> {
              if (entry.getValue() == null) {
                return null;
              }
              return lambda2.getFunction().apply(entry.getKey(), entry.getValue());
            }));
  }
}
