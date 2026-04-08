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

import io.confluent.ksql.util.KsqlConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.streams.StreamsConfig;

/**
 * Handles configuration property migrations for backward compatibility.
 * 
 * <p>This class is responsible for transforming deprecated or legacy configuration
 * values into their modern equivalents when commands are executed. Migrations are
 * applied at runtime (during command execution) but not during serialization, ensuring
 * that original values are preserved in the command topic for backup and restore scenarios.
 */
public final class ConfigMigrator {

  /**
   * The deprecated legacy value for exactly-once processing guarantee.
   * This value is no longer supported in newer Kafka Streams versions.
   */
  public static final String LEGACY_EXACTLY_ONCE = "exactly_once";

  private ConfigMigrator() {
    // Utility class
  }

  /**
   * Migrates overwrite properties (session-level overrides) for execution.
   */
  public static <T> Map<String, T> migrateOverwriteProperties(
      final Map<String, T> overwriteProperties
  ) {
    return migrateProcessingGuarantee(
        overwriteProperties,
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG
    );
  }

  /**
   * Migrates original properties (server config) for execution.
   */
  public static <T> Map<String, T> migrateOriginalProperties(
      final Map<String, T> originalProperties
  ) {
    return migrateProcessingGuarantee(
        originalProperties,
        KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG
    );
  }

  /**
   * Migrates the 'processing.guarantee' property from 'exactly_once' to 'exactly_once_v2'.
   * 
   * <p>This migration is necessary because Kafka Streams deprecated 'exactly_once' in favor
   * of 'exactly_once_v2' in newer versions. Commands stored in the command topic may contain
   * the legacy value, and this method ensures they can be executed on newer Kafka Streams
   * versions without validation errors.
   */
  private static <T> Map<String, T> migrateProcessingGuarantee(
      final Map<String, T> properties,
      final String key
  ) {
    final T value = properties.get(key);
    if (value != null && LEGACY_EXACTLY_ONCE.equals(value.toString())) {
      final Map<String, T> migrated = new HashMap<>(properties);
      @SuppressWarnings("unchecked")
      final T migratedValue = (T) StreamsConfig.EXACTLY_ONCE_V2;
      migrated.put(key, migratedValue);
      return migrated;
    }
    return properties;
  }
}


