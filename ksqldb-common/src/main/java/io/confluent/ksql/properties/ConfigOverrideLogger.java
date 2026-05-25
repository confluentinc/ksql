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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Emits an audit log line for each property override seen on a REST endpoint, tagging whether
 * the property name appears in {@link KsqlConfig#KSQL_PROPERTIES_OVERRIDES_ALLOWLIST}.
 *
 * <p>Gated on {@link KsqlConfig#KSQL_PROPERTIES_OVERRIDES_LOG}. Default disabled — flipping the
 * flag to true is the audit-mode rollout step before enforcement of allowlist of properties.
 *
 * <p>The log message carries only event-specific fields ({@code event}, {@code endpoint},
 * {@code property}, {@code inAllowlist}). Cluster / org / pod identifiers are auto-stamped by
 * the log shipper (fluentbit + k8s metadata) and surface as their own top-level OpenSearch
 * fields, so we deliberately do not duplicate them inside the message.
 */
public class ConfigOverrideLogger {

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
    Objects.requireNonNull(endpoint, "endpoint");
    if (properties == null || properties.isEmpty()) {
      log.info("event=no_property_overrides endpoint={}", endpoint);
      return;
    }
    for (final String key : properties.keySet()) {
      log.info(
          "event=property_override endpoint={} property={} inAllowlist={}",
          endpoint,
          key,
          allowlist.contains(key)
      );
    }
  }
}
