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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Emits an audit log line for each property override seen on a REST endpoint, tagging whether
 * the property name appears in {@link KsqlConfig#KSQL_PROPERTIES_OVERRIDES_ALLOWLIST}.
 *
 * <p>Gated on {@link KsqlConfig#KSQL_PROPERTIES_OVERRIDES_LOG}. Default disabled — flipping the
 * flag to true is the audit-mode rollout step before enforcement of allowlist of properties.
 */
public class ConfigOverrideLogger {

  private static final Logger LOG = LoggerFactory.getLogger("io.confluent.ksql.audit.ConfigOverride");

  private final boolean enabled;
  private final Set<String> allowlist;
  private final String clusterId;

  public ConfigOverrideLogger(final KsqlConfig config) {
    Objects.requireNonNull(config, "config");
    this.enabled = config.getBoolean(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_LOG);
    this.allowlist = ImmutableSet.copyOf(
        config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST));
    this.clusterId = config.getString(KsqlConfig.KSQL_SERVICE_ID_CONFIG);
  }

  public void logOverrides(final String endpoint, final Map<String, Object> properties) {
    if (!enabled) {
      return;
    }
    Objects.requireNonNull(endpoint, "endpoint");
    if (properties == null || properties.isEmpty()) {
      LOG.info(
          "event=no_property_overrides clusterId={} endpoint={}",
          clusterId,
          endpoint
      );
      return;
    }
    for (final String key : properties.keySet()) {
      LOG.info(
          "event=property_override clusterId={} endpoint={} property={} inAllowlist={}",
          clusterId,
          endpoint,
          key,
          allowlist.contains(key)
      );
    }
  }
}
