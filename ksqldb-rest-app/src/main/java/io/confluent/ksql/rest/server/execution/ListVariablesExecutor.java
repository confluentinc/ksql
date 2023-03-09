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
import io.confluent.ksql.parser.tree.ListVariables;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.VariablesList;
import io.confluent.ksql.rest.entity.VariablesList.Variable;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ListVariablesExecutor {
  private ListVariablesExecutor() {
  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<ListVariables> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final List<Variable> sessionVariables = sessionProperties.getSessionVariables().entrySet()
        .stream()
        .map(e -> new Variable(e.getKey(), e.getValue()))
        .collect(Collectors.toList());

    return StatementExecutorResponse.handled(Optional.of(
        new VariablesList(statement.getMaskedStatementText(), sessionVariables)));
  }
}
