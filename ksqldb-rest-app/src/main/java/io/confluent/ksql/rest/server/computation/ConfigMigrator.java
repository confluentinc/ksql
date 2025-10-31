/*
 * Copyright 2025 Confluent Inc.
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

package io.confluent.ksql.rest.server.computation;

import java.util.HashMap;
import java.util.Map;

/**
 * Handles configuration property migrations for backward compatibility.
 * 
 * <p>This class is responsible for transforming deprecated or legacy configuration
 * values into their modern equivalents when commands are executed. Migrations are
 * applied at runtime (during command execution) but not during serialization, ensuring
 * that original values are preserved in the command topic for backup and restore scenarios.
 */
public final class ConfigMigrator {

  private ConfigMigrator() {
    // Utility class
  }

  /**
   * Migrates the 'processing.guarantee' property from 'exactly_once' to 'exactly_once_v2'
   * 
   * @param properties the properties map to migrate
   * @param key the key to check for migration (e.g., "processing.guarantee" or
   *            "ksql.streams.processing.guarantee")
   * @param <T> the type of values in the map (Object for overwriteProperties, String for
   *            originalProperties)
   * @return a new map with migrated values if migration was needed, otherwise the original map
   */
  public static <T> Map<String, T> migrateProcessingGuarantee(
      final Map<String, T> properties,
      final String key
  ) {
    final T value = properties.get(key);
    if (value != null && "exactly_once".equals(value.toString())) {
      final Map<String, T> migrated = new HashMap<>(properties);
      @SuppressWarnings("unchecked")
      final T migratedValue = (T) "exactly_once_v2";
      migrated.put(key, migratedValue);
      return migrated;
    }
    return properties;
  }
}

