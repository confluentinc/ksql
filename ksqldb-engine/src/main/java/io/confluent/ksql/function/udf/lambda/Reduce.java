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

import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiFunction;

/**
 * Reduce a collection using an initial state and function
 */
@UdfDescription(
    name = "reduce",
    category = FunctionCategory.LAMBDA,
    description = "Reduce the input collection down to a single value " 
        + "using an initial state and a function. " 
        + "The initial state (s) is passed into the scope of the function. " 
        + "Each invocation returns a new value for s, " 
        + "which the next invocation will receive. "  
        + "The final value for s is returned.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class Reduce {

  @Udf(description = "When reducing an array, "
      + "the reduce function must have two arguments. "
      + "The two arguments for the reduce function are in order: "
      + "the state and the array item. "
      + "The final state is returned."
  )
  public <T,S> S reduceArray(
      @UdfParameter(description = "The array.") final List<T> list,
      @UdfParameter(description = "The initial state.") final S initialState,
      @UdfParameter(description = "The reduce function.") final BiFunction<S, T, S> biFunction
  ) {
    if (initialState == null || biFunction == null) {
      return null;
    }

    if (list == null) {
      return initialState;
    }

    S state = initialState;
    for (T listItem: list) {
      state = biFunction.apply(state, listItem);
    }
    return state;
  }

  @Udf(description = "When reducing a map, " 
      + "the reduce function must have three arguments. "
      + "The three arguments for the reduce function are in order: " 
      + "the state, the key, and the value. "
      + "The final state is returned."
  )
  public <K,V,S> S reduceMap(
      @UdfParameter(description = "The map.") final Map<K, V> map,
      @UdfParameter(description = "The initial state.") final S initialState,
      @UdfParameter(description = "The reduce function.") final TriFunction<S, K, V, S> triFunction
  ) {
    if (initialState == null || triFunction == null) {
      return null;
    }

    if (map == null) {
      return initialState;
    }

    S state = initialState;
    for (Entry<K, V> entry : map.entrySet()) {
      state = triFunction.apply(state, entry.getKey(), entry.getValue());
    }
    return state;
  }
}
