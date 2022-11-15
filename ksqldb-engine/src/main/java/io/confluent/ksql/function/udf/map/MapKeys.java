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

import com.google.common.collect.Lists;
import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import java.util.List;
import java.util.Map;

@UdfDescription(
    name = "map_keys",
    category = FunctionCategory.MAP,
    description = "Returns an array of all the keys from the specified map, "
        + "or NULL if the input map is NULL.")
public class MapKeys {

  @Udf
  public <T> List<String> mapKeys(final Map<String, T> input) {
    if (input == null) {
      return null;
    }
    return Lists.newArrayList(input.keySet());
  }

}
