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

package io.confluent.ksql.function.udf.array;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.types.KsqlLambda;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Returns the length of an array
 */
@SuppressWarnings("MethodMayBeStatic") // UDF methods can not be static.
@UdfDescription(
    name = "TRANSFORM_ARRAY",
    category = FunctionCategory.ARRAY,
    description = "Apply a lambda function to an array",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class TransformArray {

  @Udf
  public <T, R> List<R> transformArray(
      @UdfParameter(description = "The array") final List<T> array,
      @UdfParameter(description = "The lambda") final KsqlLambda<T, R> lambda
  ) {
    if (array == null) {
      return null;
    }
    return array.stream().map(item -> {
      if (item == null) {
        return null;
      }
      return lambda.getFunction().apply(item);
    }).collect(Collectors.toList());
  }
}
