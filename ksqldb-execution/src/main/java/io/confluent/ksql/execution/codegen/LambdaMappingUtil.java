/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.codegen;

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public final class LambdaMappingUtil {
  private LambdaMappingUtil() {

  }

  public static Map<String, SqlType> resolveOldAndNewLambdaMapping(
      final Map<String, SqlType> newMapping,
      final Map<String, SqlType> oldMapping
  ) {
    final HashMap<String, SqlType> updatedMapping = new HashMap<>(oldMapping);
    for (final Entry<String, SqlType> entry : newMapping.entrySet()) {
      final String key = entry.getKey();
      if (oldMapping.containsKey(key) && !oldMapping.get(key).equals(newMapping.get(key))) {
        throw new IllegalStateException(String.format(
            "Could not map type %s to lambda variable %s, "
                + "%s was already mapped to %s",
            newMapping.get(key).toString(),
            key,
            key,
            oldMapping.get(key).toString()));
      }
      updatedMapping.put(key, entry.getValue());
    }
    return updatedMapping;
  }
}
