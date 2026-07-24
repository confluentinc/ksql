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

import static io.confluent.ksql.rest.server.computation.ConfigMigrator.LEGACY_EXACTLY_ONCE;
import static io.confluent.ksql.rest.server.computation.ConfigMigrator.LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER;
import static io.confluent.ksql.rest.server.computation.ConfigMigrator.LEGACY_OAUTH_VALIDATOR_CALLBACK_HANDLER;
import static io.confluent.ksql.util.KsqlConfig.KSQL_STREAMS_PREFIX;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginCallbackHandler;
import org.apache.kafka.common.security.oauthbearer.OAuthBearerValidatorCallbackHandler;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.Test;

public class ConfigMigratorTest {

  // =========================================================================
  // Processing Guarantee Migration Tests
  // =========================================================================

  @Test
  public void shouldMigrateExactlyOnceInOverwriteProperties() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, LEGACY_EXACTLY_ONCE);
    properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2
    ));
    assertThat(result, hasEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4));
  }

  @Test
  public void shouldMigrateExactlyOnceInOriginalProperties() {
    // Given:
    final Map<String, String> properties = new HashMap<>();
    properties.put(
        KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        LEGACY_EXACTLY_ONCE
    );
    properties.put(KSQL_STREAMS_PREFIX + StreamsConfig.APPLICATION_ID_CONFIG, "my_app");

    // When:
    final Map<String, String> result = ConfigMigrator.migrateOriginalProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2
    ));
    assertThat(result, hasEntry(
        KSQL_STREAMS_PREFIX + StreamsConfig.APPLICATION_ID_CONFIG,
        "my_app"
    ));
  }

  @Test
  public void shouldNotModifyExactlyOnceV2InOverwriteProperties() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
    assertThat(
        result.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(StreamsConfig.EXACTLY_ONCE_V2)
    );
  }

  @Test
  public void shouldNotModifyExactlyOnceV2InOriginalProperties() {
    // Given:
    final Map<String, String> properties = ImmutableMap.of(
        KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2,
        KSQL_STREAMS_PREFIX + StreamsConfig.NUM_STREAM_THREADS_CONFIG, "4"
    );

    // When:
    final Map<String, String> result = ConfigMigrator.migrateOriginalProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
    assertThat(
        result.get(KSQL_STREAMS_PREFIX + StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(StreamsConfig.EXACTLY_ONCE_V2)
    );
  }

  @Test
  public void shouldNotModifyAtLeastOnce() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.AT_LEAST_ONCE,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
    assertThat(
        result.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(StreamsConfig.AT_LEAST_ONCE)
    );
  }

  // =========================================================================
  // OAuth Login Callback Handler Migration Tests
  // =========================================================================

  @Test
  public void shouldMigrateOAuthLoginHandlerInOverwriteProperties() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        OAuthBearerLoginCallbackHandler.class.getName()
    ));
  }

  @Test
  public void shouldMigrateOAuthLoginHandlerInOriginalProperties() {
    // Given:
    final Map<String, String> properties = new HashMap<>();
    properties.put(
        KSQL_STREAMS_PREFIX + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER
    );

    // When:
    final Map<String, String> result = ConfigMigrator.migrateOriginalProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        KSQL_STREAMS_PREFIX + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        OAuthBearerLoginCallbackHandler.class.getName()
    ));
  }

  @Test
  public void shouldNotModifyNewOAuthLoginHandler() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        OAuthBearerLoginCallbackHandler.class.getName()
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
  }

  // =========================================================================
  // OAuth Validator Callback Handler Migration Tests
  // =========================================================================

  @Test
  public void shouldMigrateOAuthValidatorHandlerInOverwriteProperties() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put(
        SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_VALIDATOR_CALLBACK_HANDLER
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        OAuthBearerValidatorCallbackHandler.class.getName()
    ));
  }

  @Test
  public void shouldMigrateOAuthValidatorHandlerInOriginalProperties() {
    // Given:
    final Map<String, String> properties = new HashMap<>();
    properties.put(
        KSQL_STREAMS_PREFIX + SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_VALIDATOR_CALLBACK_HANDLER
    );

    // When:
    final Map<String, String> result = ConfigMigrator.migrateOriginalProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        KSQL_STREAMS_PREFIX + SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        OAuthBearerValidatorCallbackHandler.class.getName()
    ));
  }

  @Test
  public void shouldNotModifyNewOAuthValidatorHandler() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        OAuthBearerValidatorCallbackHandler.class.getName()
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
  }

  // =========================================================================
  // Multiple Migrations Tests
  // =========================================================================

  @Test
  public void shouldMigrateMultiplePropertiesInSingleCall() {
    // Given:
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, LEGACY_EXACTLY_ONCE);
    properties.put(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER
    );
    properties.put(
        SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_VALIDATOR_CALLBACK_HANDLER
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2
    ));
    assertThat(result, hasEntry(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        OAuthBearerLoginCallbackHandler.class.getName()
    ));
    assertThat(result, hasEntry(
        SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        OAuthBearerValidatorCallbackHandler.class.getName()
    ));
  }

  // =========================================================================
  // Edge Case Tests
  // =========================================================================

  @Test
  public void shouldNotModifyWhenKeyNotPresent() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
  }

  @Test
  public void shouldHandleEmptyMap() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of();

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, sameInstance(properties));
  }

  @Test
  public void shouldPreserveOriginalMapWhenNoMigrationNeeded() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2,
        StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat("Should return same instance when no migration needed",
        result, sameInstance(properties));
  }

  @Test
  public void shouldCreateNewMapOnlyWhenMigrationNeeded() {
    // Given:
    final Map<String, Object> properties = ImmutableMap.of(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG, LEGACY_EXACTLY_ONCE
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat("Should create new map when migration needed",
        result != properties, is(true));
    assertThat(
        result.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG),
        is(StreamsConfig.EXACTLY_ONCE_V2)
    );
  }

  // =========================================================================
  // Mixed Valid and Invalid Values Tests
  // =========================================================================

  @Test
  public void shouldMigrateOnlyLegacyValuesWhenMixedWithModernValues() {
    // Given: processing.guarantee is already modern, but OAuth handlers are legacy
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    properties.put(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER
    );
    properties.put(
        SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        OAuthBearerValidatorCallbackHandler.class.getName()  // already modern
    );

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then: Only the legacy OAuth login handler should be migrated
    assertThat(result, hasEntry(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2
    ));
    assertThat(result, hasEntry(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        OAuthBearerLoginCallbackHandler.class.getName()
    ));
    assertThat(result, hasEntry(
        SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS,
        OAuthBearerValidatorCallbackHandler.class.getName()
    ));
  }

  @Test
  public void shouldNotMigrateCustomCallbackHandler() {
    // Given: User has a custom callback handler implementation
    final String customHandler = "com.mycompany.security.CustomOAuthCallbackHandler";
    final Map<String, Object> properties = new HashMap<>();
    properties.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, customHandler);

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then: Custom handler should NOT be migrated
    assertThat(result, sameInstance(properties));
    assertThat(result.get(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS), is(customHandler));
  }

  @Test
  public void shouldHandleNullValuesInProperties() {
    // Given: Properties map contains null values
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, null);
    properties.put(SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS, LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER);

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then: Null value should be preserved, legacy OAuth should be migrated
    assertThat(result.containsKey(StreamsConfig.PROCESSING_GUARANTEE_CONFIG), is(true));
    assertThat(result.get(StreamsConfig.PROCESSING_GUARANTEE_CONFIG), is((Object) null));
    assertThat(result, hasEntry(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        OAuthBearerLoginCallbackHandler.class.getName()
    ));
  }

  @Test
  public void shouldMigrateOnlyLoginHandlerWhenValidatorNotConfigured() {
    // Given: Partial OAuth setup - only login handler configured
    final Map<String, Object> properties = new HashMap<>();
    properties.put(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER
    );
    properties.put("some.other.config", "some-value");

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then:
    assertThat(result, hasEntry(
        SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        OAuthBearerLoginCallbackHandler.class.getName()
    ));
    assertThat(result, hasEntry("some.other.config", "some-value"));
    assertThat(result.containsKey(SaslConfigs.SASL_CLIENT_CALLBACK_HANDLER_CLASS), is(false));
  }

  @Test
  public void shouldNotMigrateLegacyValueInUnrelatedKey() {
    // Given: Legacy value appears in a different config key (should not be migrated)
    final Map<String, Object> properties = new HashMap<>();
    properties.put("some.custom.config", LEGACY_EXACTLY_ONCE);
    properties.put("another.custom.config", LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER);

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then: Values should NOT be migrated because they're in wrong keys
    assertThat(result, sameInstance(properties));
    assertThat(result.get("some.custom.config"), is(LEGACY_EXACTLY_ONCE));
    assertThat(result.get("another.custom.config"), is(LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER));
  }

  @Test
  public void shouldPreserveOtherPropertiesWhenMigrating() {
    // Given: Properties with various unrelated configs mixed with legacy values
    final Map<String, Object> properties = new HashMap<>();
    properties.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, LEGACY_EXACTLY_ONCE);
    properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4);
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-app");
    properties.put("custom.setting", "custom-value");
    properties.put(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER");

    // When:
    final Map<String, Object> result = ConfigMigrator.migrateOverwriteProperties(properties);

    // Then: All properties should be preserved, only legacy value migrated
    assertThat(result, hasEntry(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2
    ));
    assertThat(result, hasEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 4));
    assertThat(result, hasEntry(StreamsConfig.APPLICATION_ID_CONFIG, "my-app"));
    assertThat(result, hasEntry("custom.setting", "custom-value"));
    assertThat(result, hasEntry(SaslConfigs.SASL_MECHANISM, "OAUTHBEARER"));
  }

  @Test
  public void shouldMigrateBothOriginalAndOverwritePropertiesIndependently() {
    // Given: Different legacy values in overwrite vs original properties
    final Map<String, Object> overwrite = new HashMap<>();
    overwrite.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, LEGACY_EXACTLY_ONCE);

    final Map<String, String> original = new HashMap<>();
    original.put(
        KSQL_STREAMS_PREFIX + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        LEGACY_OAUTH_LOGIN_CALLBACK_HANDLER
    );

    // When:
    final Map<String, Object> overwriteResult = ConfigMigrator.migrateOverwriteProperties(overwrite);
    final Map<String, String> originalResult = ConfigMigrator.migrateOriginalProperties(original);

    // Then: Each should be migrated independently
    assertThat(overwriteResult, hasEntry(
        StreamsConfig.PROCESSING_GUARANTEE_CONFIG,
        StreamsConfig.EXACTLY_ONCE_V2
    ));
    assertThat(originalResult, hasEntry(
        KSQL_STREAMS_PREFIX + SaslConfigs.SASL_LOGIN_CALLBACK_HANDLER_CLASS,
        OAuthBearerLoginCallbackHandler.class.getName()
    ));
  }
}
