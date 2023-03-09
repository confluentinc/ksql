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
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Map;

public final class PropertyOverrider {

  private PropertyOverrider() {
  }

  public static void set(
      final ConfiguredStatement<SetProperty> statement,
      final Map<String, Object> mutableProperties
  ) {
    final SetProperty setProperty = statement.getStatement();
    throwIfInvalidProperty(setProperty.getPropertyName(), statement.getMaskedStatementText());
    throwIfInvalidPropertyValues(setProperty, statement);
    mutableProperties.put(setProperty.getPropertyName(), setProperty.getPropertyValue());
  }

  public static void unset(
      final ConfiguredStatement<UnsetProperty> statement,
      final Map<String, Object> mutableProperties
  ) {
    final UnsetProperty unsetProperty = statement.getStatement();
    throwIfInvalidProperty(unsetProperty.getPropertyName(), statement.getMaskedStatementText());
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
    } catch (final Exception e) {
      throw new KsqlStatementException(
          e.getMessage(), statement.getMaskedStatementText(), e.getCause());
    }
  }

  private static void throwIfInvalidProperty(final String propertyName, final String text) {
    new KsqlConfigResolver()
        .resolve(propertyName, true)
        .orElseThrow(() -> new KsqlStatementException("Unknown property: " + propertyName, text));
  }

}
