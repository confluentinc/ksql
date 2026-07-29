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

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.util.KsqlConfig;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Handles configuration property migrations for backward compatibility.
 *
 * <p>This class transforms deprecated configuration values into their modern equivalents
 * when commands are executed. Migrations are applied at runtime (during command execution)
 * but not during serialization, preserving original values in the command topic.
 *
 * <p>To add a new migration, add an entry to {@link #OVERWRITE_MIGRATIONS} and
 * {@link #ORIGINAL_MIGRATIONS}.
 */
public final class ConfigMigrator {

  private static final Logger LOG = LogManager.getLogger(ConfigMigrator.class);

  /**
   * Legacy exactly-once value, deprecated in Kafka Streams 3.0+.
   */
  public static final String LEGACY_EXACTLY_ONCE = "exactly_once";

  /**
   * Legacy OAuth login callback handler from the "secured" package.
   * Removed in Kafka 4.0/CP 8.x (deprecated since Kafka 3.4.0).
   */
  public static final String LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER =
      "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler";

  /**
   * Legacy OAuth validator callback handler from the "secured" package.
   * Removed in Kafka 4.0/CP 8.x (deprecated since Kafka 3.4.0).
   */
  public static final String LEGACY_OAUTH_VALIDATOR_CALLBACK_HANDLER =
      "org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerValidatorCallbackHandler";

  /**
   * Migrations for overwrite properties (session-level overrides).
   * Keys are unprefixed.
   */
  private static final Map<String, Map<String, String>> OVERWRITE_MIGRATIONS =
      ImmutableMap.<String, Map<String, String>>builder()
          .put(
              StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
              ImmutableMap.of(LEGACY_EXACTLY_ONCE, StreamsConfig.EXACTLY_ONCE_V2)
          )
          .put(
              SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
              ImmutableMap.of(
                  LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER,
                  OAuthBearerLoginCallbackHandler.class.getName()
              )
          )
          .put(
              SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
              ImmutableMap.of(
                  LEGACY_OAUTH_VALIDATOR_CALLBACK_HANDLER,
                  OAuthBearerValidatorCallbackHandler.class.getName()
              )
          )
          .build();

  /**
   * Migrations for original properties (server configuration).
   * Keys are prefixed with ksql.streams.
   */
  private static final Map<String, Map<String, String>> ORIGINAL_MIGRATIONS =
      ImmutableMap.<String, Map<String, String>>builder()
          .put(
              KsqlConfig.KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
              ImmutableMap.of(LEGACY_EXACTLY_ONCE, StreamsConfig.EXACTLY_ONCE_V2)
          )
          .put(
              KsqlConfig.KSQL_STREAMS_PREFIX + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
              ImmutableMap.of(
                  LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER,
                  OAuthBearerLoginCallbackHandler.class.getName()
              )
          )
          .put(
              KsqlConfig.KSQL_STREAMS_PREFIX + SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
              ImmutableMap.of(
                  LEGACY_OAUTH_VALIDATOR_CALLBACK_HANDLER,
                  OAuthBearerValidatorCallbackHandler.class.getName()
              )
          )
          .build();

  private ConfigMigrator() {
    // Utility class
  }

  /**
   * Migrates overwrite properties (session-level overrides) for execution.
   */
  public static <T> Map<String, T> migrateOverwriteProperties(
      final Map<String, T> overwriteProperties
  ) {
    return applyMigrations(overwriteProperties, OVERWRITE_MIGRATIONS);
  }

  /**
   * Migrates original properties (server config) for execution.
   */
  public static <T> Map<String, T> migrateOriginalProperties(
      final Map<String, T> originalProperties
  ) {
    return applyMigrations(originalProperties, ORIGINAL_MIGRATIONS);
  }

  /**
   * Applies migrations using O(1) HashMap lookups.
   *
   * <p>For each migration, we do a direct lookup in the properties map (O(1)),
   * then check if the value needs replacement (O(1)). Total: O(M) where M is
   * the number of migrations (constant).
   *
   * @param properties the properties map to migrate
   * @param migrations the migration definitions
   * @return the original map if no changes, or a new map with migrations applied
   */
  private static <T> Map<String, T> applyMigrations(
      final Map<String, T> properties,
      final Map<String, Map<String, String>> migrations
  ) {
    Map<String, T> result = null;

    for (final Map.Entry<String, Map<String, String>> migration : migrations.entrySet()) {
      final String key = migration.getKey();
      final T currentValue = properties.get(key);

      if (currentValue != null) {
        final String replacement = migration.getValue().get(currentValue.toString());

        if (replacement != null && !replacement.equals(currentValue.toString())) {
          LOG.info("Migrating deprecated config value for '{}': '{}' -> '{}'",
              key, currentValue, replacement);

          // Lazy initialization - only create new map when we have a change
          if (result == null) {
            result = new HashMap<>(properties);
          }

          @SuppressWarnings("unchecked")
          final T typedReplacement = (T) replacement;
          result.put(key, typedReplacement);
        }
      }
    }

    return result != null ? result : properties;
  }
}
