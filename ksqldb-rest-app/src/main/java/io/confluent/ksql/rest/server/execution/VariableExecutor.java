/*
 * Copyright 2020 Confluent Inc.
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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.parser.tree.DefineVariable;
import io.confluent.ksql.parser.tree.UndefineVariable;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.WarningEntity;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.Optional;

public final class VariableExecutor {
  private VariableExecutor() {
  }

  public static StatementExecutorResponse set(
      final ConfiguredStatement<DefineVariable> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final DefineVariable defineVariable = statement.getStatement();
    sessionProperties.setVariable(
        defineVariable.getVariableName(),
        defineVariable.getVariableValue()
    );

    return StatementExecutorResponse.handled(Optional.empty());
  }

  public static StatementExecutorResponse unset(
      final ConfiguredStatement<UndefineVariable> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final String variableName = statement.getStatement().getVariableName();

    if (!sessionProperties.getSessionVariables().containsKey(variableName)) {
      return StatementExecutorResponse.handled(Optional.of(new WarningEntity(
          statement.getMaskedStatementText(),
          String.format("Cannot undefine variable '%s' which was never defined", variableName)
      )));
    }

    sessionProperties.unsetVariable(variableName);

    return StatementExecutorResponse.handled(Optional.empty());
  }
}
