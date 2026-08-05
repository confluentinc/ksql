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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.CloseableThreadContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Applies name-only filtering to property overrides found while restoring the command topic.
 * Gated on {@link KsqlConfig#KSQL_PROPERTIES_OVERRIDES_VALIDATION_RESTORE_ENABLED}.
 *
 * <p>Exactly one list applies to filter the property overrides
 * <ul>
 *   <li>{@code denylist} (the default, and what an unset mode resolves to) - drop keys that are
 *       on the denylist, keep everything else;</li>
 *   <li>{@code allowlist} - drop keys that are not on the allowlist.</li>
 * </ul>
 *
 * <p>Names only - values are neither validated nor logged.
 */
public class RestorePropertyOverrideFilter {

  private static final Logger LOG = LogManager.getLogger(RestorePropertyOverrideFilter.class);

  private static final String ENDPOINT = "endpoint";
  private static final String QUERY = "query";
  private static final String PROPERTY = "property";
  private static final String MODE = "mode";

  /** The configured mode, as configured - logged verbatim so a typo is visible. */
  private final String mode;

  private final boolean allowlistMode;
  private final Set<String> activeList;

  public RestorePropertyOverrideFilter(final KsqlConfig config) {
    Objects.requireNonNull(config, "config");

    this.mode = config.getString(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE);
    this.allowlistMode =
        KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE_ALLOWLIST.equals(mode);

    if (allowlistMode) {
      this.activeList = ImmutableSet.copyOf(
          config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST));
    } else {
      this.activeList = ImmutableSet.copyOf(
          config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST));
    }
  }

  /**
   * In allowlist mode the active list is a keep-list, so anything absent from it is excluded.
   * In denylist mode (the default) it is a drop-list, so anything present in it is excluded.
   */
  private boolean isExcluded(final String key) {
    if (allowlistMode) {
      return !activeList.contains(key);
    }
    return activeList.contains(key);
  }

  /**
   * Drops overrides rejected by the active list. Never mutates {@code overrides}.
   * Emits one WARN per key dropped.
   * @return a new map containing only the surviving entries.
   */
  public Map<String, Object> filter(
      final String endpoint,
      final String query,
      final Map<String, Object> overrides
  ) {
    final Map<String, Object> result = new HashMap<>();
    for (final Map.Entry<String, Object> entry : overrides.entrySet()) {
      final String key = entry.getKey();

      if (isExcluded(key)) {
        try (CloseableThreadContext.Instance ignored = CloseableThreadContext
            .put(ENDPOINT, endpoint)
            .put(QUERY, query)
            .put(PROPERTY, key)
            .put(MODE, mode)) {
          LOG.warn("Config override excluded from restore");
        }
        continue;
      }

      result.put(key, entry.getValue());
    }
    return result;
  }
}
