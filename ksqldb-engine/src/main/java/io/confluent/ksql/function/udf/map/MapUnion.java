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

package io.confluent.ksql.function.udf.map;

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@UdfDescription(
    name = "map_union",
    category = FunctionCategory.MAP,
    description = "Returns a new map containing the union of all entries from both input maps. "
        + "If a key is present in both input maps then the value from map2 is the one which "
        + "appears in the result. Returns NULL if all of the input maps are NULL.")
public class MapUnion {

  @Udf
  public <T> Map<String, T> union(
      @UdfParameter(description = "first map to union") final Map<String, T> map1,
      @UdfParameter(description = "second map to union") final Map<String, T> map2) {

    final List<Map<String, T>> nonNullInputs =
        Stream.of(map1, map2)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
    if (nonNullInputs.size() == 0) {
      return null;
    }

    final Map<String, T> output = new HashMap<>();
    nonNullInputs
        .forEach(output::putAll);
    return output;
  }
}
