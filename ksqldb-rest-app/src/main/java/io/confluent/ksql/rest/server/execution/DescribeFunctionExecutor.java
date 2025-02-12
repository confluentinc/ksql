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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.KsqlExecutionContext;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.ParameterInfo;
import io.confluent.ksql.function.TableFunctionFactory;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.rest.SessionProperties;
import io.confluent.ksql.rest.entity.ArgumentInfo;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionInfo;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.IdentifierUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public final class DescribeFunctionExecutor {

  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(IdentifierUtil::needsQuotes);

  private DescribeFunctionExecutor() {

  }

  public static StatementExecutorResponse execute(
      final ConfiguredStatement<DescribeFunction> statement,
      final SessionProperties sessionProperties,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final DescribeFunction describeFunction = statement.getStatement();
    final FunctionName functionName = FunctionName.of(describeFunction.getFunctionName());

    if (executionContext.getMetaStore().isAggregate(functionName)) {
      return StatementExecutorResponse.handled(Optional.of(
          describeAggregateFunction(executionContext, functionName,
              statement.getMaskedStatementText())));
    }

    if (executionContext.getMetaStore().isTableFunction(functionName)) {
      return StatementExecutorResponse.handled(Optional.of(
          describeTableFunction(executionContext, functionName,
              statement.getMaskedStatementText())));
    }

    return StatementExecutorResponse.handled(Optional.of(
        describeNonAggregateFunction(executionContext, functionName,
            statement.getMaskedStatementText())));
  }

  private static FunctionDescriptionList describeAggregateFunction(
      final KsqlExecutionContext ksqlEngine,
      final FunctionName functionName,
      final String statementText
  ) {
    final AggregateFunctionFactory aggregateFactory
        = ksqlEngine.getMetaStore().getAggregateFactory(functionName);

    final ImmutableList.Builder<FunctionInfo> listBuilder = ImmutableList.builder();

    aggregateFactory.eachFunction((func, description) -> listBuilder.add(
        getFunctionInfo(
            func.parameterInfo(), func.declaredReturnType(), description, false)));

    return createFunctionDescriptionList(
        statementText, aggregateFactory.getMetadata(), listBuilder.build(), FunctionType.AGGREGATE);
  }

  private static FunctionDescriptionList describeTableFunction(
      final KsqlExecutionContext executionContext,
      final FunctionName functionName,
      final String statementText
  ) {
    final TableFunctionFactory tableFunctionFactory = executionContext.getMetaStore()
        .getTableFunctionFactory(functionName);

    final ImmutableList.Builder<FunctionInfo> listBuilder = ImmutableList.builder();

    tableFunctionFactory.eachFunction(func -> listBuilder.add(
        getFunctionInfo(
            func.parameterInfo(),
            func.declaredReturnType(),
            func.getDescription(),
            func.isVariadic()
        )));

    return createFunctionDescriptionList(
        statementText,
        tableFunctionFactory.getMetadata(),
        listBuilder.build(),
        FunctionType.TABLE
    );
  }

  private static FunctionDescriptionList describeNonAggregateFunction(
      final KsqlExecutionContext executionContext,
      final FunctionName functionName,
      final String statementText
  ) {
    final UdfFactory udfFactory = executionContext.getMetaStore().getUdfFactory(functionName);

    final ImmutableList.Builder<FunctionInfo> listBuilder = ImmutableList.builder();

    udfFactory.eachFunction(func -> listBuilder.add(
        getFunctionInfo(
            func.parameterInfo(),
            func.declaredReturnType(),
            func.getDescription(),
            func.isVariadic()
        )));

    return createFunctionDescriptionList(
        statementText,
        udfFactory.getMetadata(),
        listBuilder.build(),
        FunctionType.SCALAR
    );
  }

  private static FunctionInfo getFunctionInfo(
      final List<ParameterInfo> argTypes,
      final ParamType returnTypeSchema,
      final String description,
      final boolean variadic
  ) {
    final List<ArgumentInfo> args = new ArrayList<>();
    for (int i = 0; i < argTypes.size(); i++) {
      final ParameterInfo param = argTypes.get(i);
      final boolean isVariadic = variadic && i == (argTypes.size() - 1);
      final String type = isVariadic
          ? ((ArrayType) param.type()).element().toString()
          : param.type().toString();
      args.add(new ArgumentInfo(param.name(), type, param.description(), isVariadic));
    }

    return new FunctionInfo(args, returnTypeSchema.toString(), description);
  }

  private static FunctionDescriptionList createFunctionDescriptionList(
      final String statementText,
      final UdfMetadata metadata, final List<FunctionInfo> functionInfos,
      final FunctionType functionType
  ) {
    return new FunctionDescriptionList(
        statementText,
        metadata.getName().toUpperCase(),
        metadata.getDescription(),
        metadata.getAuthor(),
        metadata.getVersion(),
        metadata.getPath(),
        functionInfos,
        functionType
    );
  }

}
