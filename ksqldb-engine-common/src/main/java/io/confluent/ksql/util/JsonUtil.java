/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.util;

import io.vertx.core.json.JsonObject;
import java.util.HashMap;
import java.util.Map;

public final class JsonUtil {

  private JsonUtil() {
  }

  public static JsonObject convertJsonFieldCase(final JsonObject obj) {
    final Map<String, Object> convertedMap = new HashMap<>();
    for (Map.Entry<String, Object> entry : obj.getMap().entrySet()) {
      final String key;
      try {
        key = Identifiers.getIdentifierText(entry.getKey());
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(String.format(
            "Invalid column / struct field name. Name: %s. Reason: %s.",
            entry.getKey(),
            e.getMessage()
        ));
      }
      if (convertedMap.containsKey(key)) {
        throw new IllegalArgumentException("Found duplicate column / struct field name: " + key);
      }
      convertedMap.put(key, entry.getValue());
    }
    return new JsonObject(convertedMap);
  }
}
