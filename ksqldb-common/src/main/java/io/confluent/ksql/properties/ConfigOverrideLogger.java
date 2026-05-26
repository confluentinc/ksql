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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
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
public class ConfigOverrideLogger {

  private static final String ENDPOINT = "endpoint";
  private static final String PROPERTY = "property";
  private static final String MDC_IN_ALLOWLIST = "inAllowlist";

  private final Logger log = LogManager.getLogger(ConfigOverrideLogger.class);
  private final boolean enabled;
  private final Set<String> allowlist;

  public ConfigOverrideLogger(final KsqlConfig config) {
    Objects.requireNonNull(config, "config");
    this.enabled = config.getBoolean(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG);
    this.allowlist = ImmutableSet.copyOf(
        config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST));
  }

  public void logOverrides(final String endpoint, final Map<String, Object> properties) {
    if (!enabled) {
      return;
    }
    Objects.requireNonNull(endpoint, ENDPOINT);
    if (properties == null || properties.isEmpty()) {
      try (CloseableThreadContext.Instance ignored = CloseableThreadContext
          .put(ENDPOINT, endpoint)) {
        log.info("No Config overrides");
      }
      return;
    }
    for (final String key : properties.keySet()) {
      try (CloseableThreadContext.Instance ignored = CloseableThreadContext
          .put(ENDPOINT, endpoint)
          .put(PROPERTY, key)
          .put(MDC_IN_ALLOWLIST, String.valueOf(allowlist.contains(key)))) {
        log.info("Config overrides found");
      }
    }
  }
}
