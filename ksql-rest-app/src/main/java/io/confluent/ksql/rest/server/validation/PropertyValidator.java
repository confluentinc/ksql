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
import io.confluent.ksql.config.KsqlConfigResolver;
import io.confluent.ksql.parser.tree.SetProperty;
import io.confluent.ksql.parser.tree.UnsetProperty;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.KsqlStatementException;

public final class PropertyValidator {

  private PropertyValidator() { }

  @SuppressFBWarnings("RV_RETURN_VALUE_IGNORED_INFERRED")
  public static void set(
      final ConfiguredStatement<?> statement,
      final KsqlExecutionContext context,
      final ServiceContext serviceContext
  ) {
    final SetProperty setProperty = (SetProperty) statement.getStatement();
    throwIfUnknownProperty(
        setProperty.getPropertyName(),
        statement.getStatementText()
    );

    try {
      statement.getConfig().cloneWithPropertyOverwrite(ImmutableMap.of(
          setProperty.getPropertyName(),
          setProperty.getPropertyValue()
      ));
    } catch (final Exception e) {
      throw new KsqlStatementException(
          e.getMessage(), statement.getStatementText(), e.getCause());
    }

    context.execute(statement);
  }

  public static void unset(
      final ConfiguredStatement<?> statement,
      final KsqlExecutionContext context,
      final ServiceContext serviceContext
  ) {
    final UnsetProperty unsetProperty = (UnsetProperty) statement.getStatement();
    throwIfUnknownProperty(
        unsetProperty.getPropertyName(),
        statement.getStatementText()
    );
    context.execute(statement);
  }

  private static void throwIfUnknownProperty(final String propertyName, final String text) {
    new KsqlConfigResolver().resolve(propertyName, false).orElseThrow(
        () -> new KsqlStatementException("Unknown property: " + propertyName, text)
    );
  }

}
