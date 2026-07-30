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
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.MDC;

/**
 * Logs each property override at a REST endpoint. Gated on
 * {@link KsqlConfig#KSQL_PROPERTIES_OVERRIDES_LOG} (default off).
 *
 * <p>The message ({@code "Config overrides found"} / {@code "No Config overrides"}) identifies
 * the event; variable fields ({@code endpoint}, {@code property}, {@code inAllowlist}) attach
 * via log4j {@link MDC}, so JSON layouts surface them as discrete indexable fields. The keys
 * are removed after each call so they don't leak across requests on shared worker threads.
 *
 * <p>Property values are never logged — some keys (e.g. {@code sasl.jaas.config}) carry
 * credentials.
 */
public final class ConfigOverrideLogger {

  private static final Logger LOG = LogManager.getLogger(ConfigOverrideLogger.class);

  private static final String ENDPOINT = "endpoint";
  private static final String PROPERTY = "property";
  private static final String IN_ALLOWLIST = "inAllowlist";

  private static volatile boolean enabled = false;
  private static volatile Set<String> allowlist = ImmutableSet.of();

  private ConfigOverrideLogger() {
  }

  public static void configure(final KsqlConfig config) {
    enabled = config.getBoolean(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG);
    allowlist = ImmutableSet.copyOf(
        config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST));
  }

  @VisibleForTesting
  public static void reset() {
    enabled = false;
    allowlist = ImmutableSet.of();
  }

  public static void logOverrides(final String endpoint, final Map<String, Object> properties) {
    if (!enabled) {
      return;
    }
    if (properties == null || properties.isEmpty()) {
      MDC.put(ENDPOINT, endpoint);
      try {
        LOG.debug("No Config overrides");
      } finally {
        MDC.remove(ENDPOINT);
      }
      return;
    }
    for (final String key : properties.keySet()) {
      MDC.put(ENDPOINT, endpoint);
      MDC.put(PROPERTY, key);
      MDC.put(IN_ALLOWLIST, String.valueOf(allowlist.contains(key)));
      try {
        LOG.info("Config overrides found");
      } finally {
        MDC.remove(ENDPOINT);
        MDC.remove(PROPERTY);
        MDC.remove(IN_ALLOWLIST);
      }
    }
  }
}
