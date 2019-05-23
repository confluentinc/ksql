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

package io.confluent.ksql.rest.server.validation;

import com.google.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.rest.client.properties.LocalPropertyValidator;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlStatementException;
import java.util.Optional;

public final class PropertyOverrider {

  private PropertyOverrider() { }

  public static Optional<KsqlEntity> set(
      final ConfiguredStatement<SetProperty> statement,
      final KsqlExecutionContext context,
      final ServiceContext serviceContext
  ) {
    if (statement.getStatement() != null) {
      final SetProperty setProperty = statement.getStatement();
      throwIfInvalidProperty(setProperty.getPropertyName(), statement.getStatementText());
      throwIfInvalidPropertyValues(setProperty, statement);
      statement.getOverrides().put(setProperty.getPropertyName(), setProperty.getPropertyValue());
    }

    return Optional.empty();
  }

  public static Optional<KsqlEntity> unset(
      final ConfiguredStatement<UnsetProperty> statement,
      final KsqlExecutionContext context,
      final ServiceContext serviceContext
  ) {
    final UnsetProperty unsetProperty = statement.getStatement();
    throwIfInvalidProperty(unsetProperty.getPropertyName(), statement.getStatementText());
    statement.getOverrides().remove(unsetProperty.getPropertyName());
    return Optional.empty();
  }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED") // clone has side-effects
  private static void throwIfInvalidPropertyValues(
      final SetProperty setProperty,
      final ConfiguredStatement<SetProperty> statement) {
    try {
      statement.getConfig().cloneWithPropertyOverwrite(ImmutableMap.of(
          setProperty.getPropertyName(),
          setProperty.getPropertyValue()
      ));
    } catch (final Exception e) {
      throw new KsqlStatementException(
          e.getMessage(), statement.getStatementText(), e.getCause());
    }
  }

  private static void throwIfInvalidProperty(final String propertyName, final String text) {
    if (!LocalPropertyValidator.CONFIG_PROPERTY_WHITELIST.contains(propertyName)) {
      throw new KsqlStatementException("Unknown property: " + propertyName, text);
    }
  }

}
