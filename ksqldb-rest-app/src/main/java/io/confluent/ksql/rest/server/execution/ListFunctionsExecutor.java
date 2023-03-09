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

package io.confluent.ksql.rest.server.execution;

import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.ListFunctions;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.FunctionNameList;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.SimpleFunctionInfo;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ListFunctionsExecutor {

  private ListFunctionsExecutor() {

  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<ListFunctions> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final FunctionRegistry functionRegistry = executionContext.getMetaStore();

    final List<SimpleFunctionInfo> all = functionRegistry.listFunctions().stream()
        .map(factory -> new SimpleFunctionInfo(
            factory.getName().toUpperCase(),
            FunctionType.SCALAR, 
            factory.getMetadata().getCategory()
        ))
        .collect(Collectors.toList());

    functionRegistry.listTableFunctions().stream()
        .map(factory -> new SimpleFunctionInfo(
            factory.getName().toUpperCase(),
            FunctionType.TABLE, 
            factory.getMetadata().getCategory()
        ))
        .forEach(all::add);

    functionRegistry.listAggregateFunctions().stream()
        .map(factory -> new SimpleFunctionInfo(
            factory.getName().toUpperCase(),
            FunctionType.AGGREGATE, 
            factory.getMetadata().getCategory()
        ))
        .forEach(all::add);

    return StatementExecutorResponse.handled(
        Optional.of(new FunctionNameList(statement.getMaskedStatementText(), all)));
  }

}
