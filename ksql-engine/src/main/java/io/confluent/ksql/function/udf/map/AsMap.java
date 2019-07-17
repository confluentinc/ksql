/*
 * Copyright 2019 Confluent Inc.
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

import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlPreconditions;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@UdfDescription(name = "AS_MAP", description = "Construct a list based on some inputs")
public class AsMap {

  @Udf
  public final <T> Map<String, T> asMap(
      @UdfParameter final List<String> keys,
      @UdfParameter final List<T> values) {
    KsqlPreconditions.checkArgument(
        keys.size() == values.size(),
        "There must be a one to one mapping between keys and values to create a map!");
    final Map<String, T> map = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      map.putIfAbsent(keys.get(i), values.get(i));
    }
    return map;
  }

}