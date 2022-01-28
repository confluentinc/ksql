/*
 * Copyright 2019 Confluent Inc.
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

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public final class PropertyOverrider {

  private static final Set<String> QUERY_LEVEL_CONFIGS = new HashSet<>(Arrays.asList(
      "auto.offset.reset",
      "buffered.records.per.partition",
      "cache.max.bytes.buffering",
      "default.deserialization.exception.handler",
      "default.timestamp.extractor",
      "max.task.idle.ms",
      "task.timeout.ms",
      "topology.optimization"));

  private PropertyOverrider() {
  }

  public static void set(
      final ConfiguredStatement<SetProperty> statement,
      final Map<String, Object> mutableProperties
  ) {
    final SetProperty setProperty = statement.getStatement();
    throwIfInvalidProperty(setProperty.getPropertyName(), statement.getStatementText());
    throwIfInvalidPropertyValues(setProperty, statement);
    mutableProperties.put(setProperty.getPropertyName(), setProperty.getPropertyValue());
  }

  public static void unset(
      final ConfiguredStatement<UnsetProperty> statement,
      final Map<String, Object> mutableProperties
  ) {
    final UnsetProperty unsetProperty = statement.getStatement();
    throwIfInvalidProperty(unsetProperty.getPropertyName(), statement.getStatementText());
    mutableProperties.remove(unsetProperty.getPropertyName());
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED") // clone has side-effects
  private static void throwIfInvalidPropertyValues(
      final SetProperty setProperty,
      final ConfiguredStatement<SetProperty> statement) {
    try {
      statement
          .getSessionConfig()
          .getConfig(false)
          .cloneWithPropertyOverwrite(ImmutableMap.of(
              setProperty.getPropertyName(),
              setProperty.getPropertyValue()
          ));
      if (statement
          .getSessionConfig()
          .getConfig(true)
          .getBoolean(KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED)
          && !QUERY_LEVEL_CONFIGS.contains(setProperty.getPropertyName())) {
        throw new KsqlException(String.format("%s is not a settable property at the query"
                + " level with %s on. Please use ALTER SYSTEM to set %s for the cluster.",
            setProperty.getPropertyName(),
            KsqlConfig.KSQL_SHARED_RUNTIME_ENABLED,
            setProperty.getPropertyName())
        );
      }
    } catch (final Exception e) {
      throw new KsqlStatementException(
          e.getMessage(), statement.getStatementText(), e.getCause());
    }
  }

  private static void throwIfInvalidProperty(final String propertyName, final String text) {
    new KsqlConfigResolver()
        .resolve(propertyName, true)
        .orElseThrow(() -> new KsqlStatementException("Unknown property: " + propertyName, text));
  }
}
