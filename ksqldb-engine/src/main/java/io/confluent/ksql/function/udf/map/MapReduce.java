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

import io.confluent.ksql.execution.codegen.helpers.TriFunction;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Reduce a map using an initial state and function
 */
@UdfDescription(
    name = "map_reduce",
    category = FunctionCategory.MAP,
    description = "Reduce the input map down to a single value " 
        + "using an initial state and a function. " 
        + "The initial state (s) and is passed into the scope of the function. " 
        + "Each invocation returns a new value for s, which the next invocation will receive."  
        + "The final value for s is returned."
        + "The three arguments for the function are in order: key, value, state.",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class MapReduce {

  @Udf
  public <K,V,S> S mapReduce(
      @UdfParameter(description = "The map") final Map<K, V> map,
      @UdfParameter(description = "The initial state") final S initialState,
      @UdfParameter(description = "The reduce function") final TriFunction<K, V, S, S> triFunction
  ) {
    if (map == null) {
      return null;
    }

    S state = initialState;
    for (Entry<K, V> entry : map.entrySet()) {
      state = triFunction.apply(entry.getKey(), entry.getValue(), state);
    }
    return state;
  }
}
