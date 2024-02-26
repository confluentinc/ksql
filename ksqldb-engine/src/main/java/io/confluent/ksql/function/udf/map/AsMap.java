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

import io.confluent.ksql.function.FunctionCategory;
import io.confluent.ksql.function.udf.Udf;
import io.confluent.ksql.function.udf.UdfDescription;
import io.confluent.ksql.function.udf.UdfParameter;
import io.confluent.ksql.util.KsqlConstants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@UdfDescription(
    name = "AS_MAP",
    category = FunctionCategory.MAP,
    description = "Construct a list based on some inputs",
    author = KsqlConstants.CONFLUENT_AUTHOR
)
public class AsMap {

  @Udf
  public final <T> Map<String, T> asMap(
      @UdfParameter final List<String> keys,
      @UdfParameter final List<T> values) {
    if (keys == null || values == null) {
      return null;
    }
    final Map<String, T> map = new HashMap<>(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      final String key = keys.get(i);
      final T value = i >= values.size() ? null : values.get(i);

      map.put(key, value);
    }
    return map;
  }
}
