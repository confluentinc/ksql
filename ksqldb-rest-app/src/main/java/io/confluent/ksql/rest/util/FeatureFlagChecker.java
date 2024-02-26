/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.rest.util;

import io.confluent.ksql.config.SessionConfig;
import io.confluent.ksql.parser.tree.ColumnConstraints;
import io.confluent.ksql.parser.tree.CreateSource;
import io.confluent.ksql.parser.tree.TableElement;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;

/**
 * This class checks for disabled feature flags in the system. This helper class is useful to
 * validate statements and other requests that should not be executed if a feature flag is
 * disabled.
 * </p>
 * A feature flag is a configuration property in the {@link KsqlConfig} that enables or disables
 * a feature in the system. For instance, a feature flag may disable a statement syntax, such as
 * the use of HEADERS columns in a CREATE statement.
 * </p>
 * Some features flags should not allow overrides. This class does not check whether the
 * client overrides the config instead of the server. To prevent users override such feature flag,
 * then the config must be added to the {@link io.confluent.ksql.config.ImmutableProperties} class.
 */
public final class FeatureFlagChecker {
  private FeatureFlagChecker() {
  }

  /**
   * Throws an exception if at least one feature flag is disabled for the specified statement.
   * </p>
   * @param statement The statement to validate. The statement is a {@link ConfiguredStatement}
   *                  that contains the session configuration.
   */
  public static void throwOnDisabledFeatures(final ConfiguredStatement<?> statement) {
    final SessionConfig sessionConfig = statement.getSessionConfig();

    if (statement.getStatement() instanceof CreateSource) {
      throwOnDisabledFeatures((CreateSource) statement.getStatement(), sessionConfig);
    }
  }

  /**
   * Throws an exception if at least one feature flag is disabled for the specified CreateSource
   * statement.
   * </p>
   * @param createSource The CreateSource statement to validate.
   * @param sessionConfig The session configuration that may contain a feature flag.
   */
  private static void throwOnDisabledFeatures(
      final CreateSource createSource,
      final SessionConfig sessionConfig
  ) {
    final KsqlConfig ksqlConfig = sessionConfig.getConfig(false);

    if (!ksqlConfig.getBoolean(KsqlConfig.KSQL_HEADERS_COLUMNS_ENABLED)) {
      final boolean anyHeaderFound = createSource.getElements().stream()
          .map(TableElement::getConstraints)
          .anyMatch(ColumnConstraints::isHeaders);

      if (anyHeaderFound) {
        throw new KsqlException(String.format(
            "Cannot create %s because schema with headers columns is disabled.",
            createSource.getSourceType()));
      }
    }
  }
}
