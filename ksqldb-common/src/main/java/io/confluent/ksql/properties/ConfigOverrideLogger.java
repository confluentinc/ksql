/*
 * Copyright 2026 Confluent Inc.
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

package io.confluent.ksql.properties;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Logs each property override at a REST endpoint. Gated on
 * {@link KsqlConfig#KSQL_PROPERTIES_OVERRIDES_LOG} (default off).
 *
 * <p>The message ({@code "Config overrides found"} / {@code "No Config overrides"}) identifies
 * the event; variable fields ({@code endpoint}, {@code property}, {@code inAllowlist}) attach
 * via log4j2 ThreadContext, so JSON layouts surface them as discrete indexable fields.
 * {@link CloseableThreadContext} clears the keys after each call so they don't leak across
 * requests on shared worker threads.
 *
 * <p>Property values are never logged — some keys (e.g. {@code sasl.jaas.config}) carry
 * credentials.
 */
public final class ConfigOverrideLogger {

  private static final Logger LOG = LogManager.getLogger(ConfigOverrideLogger.class);

  private static final String ENDPOINT = "endpoint";
  private static final String PROPERTY = "property";
  private static final String IN_ALLOWLIST = "inAllowlist";
  private static final String QUERY = "query";
  private static final String VALUE = "value";

  /**
   * Range checks, keyed by property name. Each returns a description of how the value is out of
   * range, or {@link Optional#empty()} if it is fine. One check per property that needs one.
   */
  private static final Map<String, Function<Object, Optional<String>>> RANGE_CHECKS =
      ImmutableMap.<String, Function<Object, Optional<String>>>builder()
      .put(KsqlConfig.KSQL_QUERY_RETRY_BACKOFF_INITIAL_MS,
          ConfigOverrideLogger::checkRetryBackoffInitialMs)
      .build();

  private static volatile boolean overridesLogEnabled = false;
  private static volatile boolean rangeValidationLogEnabled = false;
  private static volatile Set<String> allowlist = ImmutableSet.of();

  private ConfigOverrideLogger() {
  }

  public static void configure(final KsqlConfig config) {
    overridesLogEnabled = config.getBoolean(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG);
    rangeValidationLogEnabled = config.getBoolean(
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_RANGE_VALIDATION_LOG_ENABLED);
    allowlist = ImmutableSet.copyOf(
        config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST));
  }

  @VisibleForTesting
  public static void reset() {
    overridesLogEnabled = false;
    rangeValidationLogEnabled = false;
    allowlist = ImmutableSet.of();
  }

  public static void logOverrides(final String endpoint, final Map<String, Object> properties) {
    logOverrides(endpoint, Optional.empty(), properties);
  }

  public static void logOverrides(
          final String endpoint,
          final Optional<String> query,
          final Map<String, Object> properties) {
    if (!overridesLogEnabled) {
      return;
    }
    if (properties == null || properties.isEmpty()) {
      try (CloseableThreadContext.Instance ignored = CloseableThreadContext
          .put(ENDPOINT, endpoint)) {
        LOG.debug("No Config overrides");
      }
      return;
    }
    for (final String key : properties.keySet()) {
      final CloseableThreadContext.Instance context = CloseableThreadContext
          .put(ENDPOINT, endpoint)
          .put(PROPERTY, key)
          .put(IN_ALLOWLIST, String.valueOf(allowlist.contains(key)));
      query.ifPresent(id -> context.put(QUERY, id));

      try (CloseableThreadContext.Instance ignored = context) {
        LOG.info("Config overrides found");
      }
    }
  }

  /**
   * Logs a WARN for each override whose value is out of range.
   * @param properties Property overrides.
   */
  public static void logRangeViolations(
      final String endpoint,
      final Map<String, Object> properties
  ) {
    logRangeViolations(endpoint, Optional.empty(), properties);
  }

  /**
   * As {@link #logRangeViolations(String, Map)}, additionally naming the query the overrides
   * belong to. Only the command topic restore path knows a query id, since it is assigned when
   * the command is first written.
   */
  public static void logRangeViolations(
      final String endpoint,
      final Optional<String> query,
      final Map<String, Object> properties
  ) {
    if (!rangeValidationLogEnabled || properties == null) {
      return;
    }

    for (final Map.Entry<String, Object> entry : properties.entrySet()) {
      final Function<Object, Optional<String>> check = RANGE_CHECKS.get(entry.getKey());
      if (check == null) {
        continue;
      }

      final Optional<String> violation = check.apply(entry.getValue());
      if (!violation.isPresent()) {
        continue;
      }

      final CloseableThreadContext.Instance context = CloseableThreadContext
          .put(ENDPOINT, endpoint)
          .put(PROPERTY, entry.getKey())
          .put(VALUE, String.valueOf(entry.getValue()));
      query.ifPresent(id -> context.put(QUERY, id));

      try (CloseableThreadContext.Instance ignored = context) {
        LOG.warn("Config override outside intended range: {}", violation.get());
      }
    }
  }

  /**
   * {@code ConfigDef} sets no lower bound on this property, so a negative backoff - which makes
   * no operational sense - passes through untouched today.
   */
  private static Optional<String> checkRetryBackoffInitialMs(final Object value) {
    if (value == null) {
      return Optional.empty();
    }

    final long backoffMs = Long.parseLong(String.valueOf(value).trim());

    if (backoffMs < 0) {
      return Optional.of("must be >= 0");
    }
    return Optional.empty();
  }
}
