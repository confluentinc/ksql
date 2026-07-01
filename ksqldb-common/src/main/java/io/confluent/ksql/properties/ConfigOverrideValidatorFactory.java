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

import io.confluent.ksql.util.KsqlConfig;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects the active {@link ConfigOverrideValidator} for a server from
 * {@code ksql.properties.overrides.validation.mode}.
 *
 * <p>Exactly one validator runs per server. When the mode is {@code allowlist} the denylist is not
 * consulted at all (allowlist wins); when it is {@code denylist} (the default) the allowlist is not
 * consulted. Selection is driven solely by the mode &mdash; a populated allowlist does not
 * implicitly activate allowlist enforcement.
 */
public final class ConfigOverrideValidatorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigOverrideValidatorFactory.class);

  private ConfigOverrideValidatorFactory() {
  }

  public static ConfigOverrideValidator forMode(final KsqlConfig config) {
    final String mode = config.getString(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE);

    if (KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE_ALLOWLIST.equals(mode)) {
      final List<String> allowlist =
          config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST);
      if (allowlist.isEmpty()) {
        LOG.warn("Property override validation mode is '{}' but '{}' is empty; all property "
                + "overrides will be rejected.",
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_VALIDATION_MODE_ALLOWLIST,
            KsqlConfig.KSQL_PROPERTIES_OVERRIDES_ALLOWLIST);
      }
      return new AllowListPropertyValidator(allowlist);
    }

    return new DenyListPropertyValidator(
        config.getList(KsqlConfig.KSQL_PROPERTIES_OVERRIDES_DENYLIST));
  }
}
