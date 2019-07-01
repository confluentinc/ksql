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
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.parser.tree.DescribeFunction;
import io.confluent.ksql.rest.entity.ArgumentInfo;
import io.confluent.ksql.rest.entity.FunctionDescriptionList;
import io.confluent.ksql.rest.entity.FunctionInfo;
import io.confluent.ksql.rest.entity.FunctionType;
import io.confluent.ksql.rest.entity.KsqlEntity;
import io.confluent.ksql.schema.connect.SqlSchemaFormatter;
import io.confluent.ksql.services.ServiceContext;
import io.confluent.ksql.statement.ConfiguredStatement;
import io.confluent.ksql.util.ParserUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Schema;

public final class DescribeFunctionExecutor {

  private static final SqlSchemaFormatter FORMATTER =
      new SqlSchemaFormatter(ParserUtil::isReservedIdentifier);

  private DescribeFunctionExecutor() { }

  public static Optional<KsqlEntity> execute(
      final ConfiguredStatement<DescribeFunction> statement,
      final KsqlExecutionContext executionContext,
      final ServiceContext serviceContext
  ) {
    final DescribeFunction describeFunction = statement.getStatement();
    final String functionName = describeFunction.getFunctionName();

    if (executionContext.getMetaStore().isAggregate(functionName)) {
      return Optional.of(
          describeAggregateFunction(executionContext, functionName, statement.getStatementText()));
    }

    return Optional.of(
        describeNonAggregateFunction(executionContext, functionName, statement.getStatementText()));
  }

  private static FunctionDescriptionList describeAggregateFunction(
      final KsqlExecutionContext ksqlEngine,
      final String functionName,
      final String statementText
  ) {
    final AggregateFunctionFactory aggregateFactory
        = ksqlEngine.getMetaStore().getAggregateFactory(functionName);

    final ImmutableList.Builder<FunctionInfo> listBuilder = ImmutableList.builder();

    aggregateFactory.eachFunction(func -> listBuilder.add(
        getFunctionInfo(func.getArguments(), func.getReturnType(), func.getDescription(), false)));

    return new FunctionDescriptionList(
        statementText,
        aggregateFactory.getName().toUpperCase(),
        aggregateFactory.getDescription(),
        aggregateFactory.getAuthor(),
        aggregateFactory.getVersion(),
        aggregateFactory.getPath(),
        listBuilder.build(),
        FunctionType.aggregate
    );
  }

  private static FunctionDescriptionList describeNonAggregateFunction(
      final KsqlExecutionContext executionContext,
      final String functionName,
      final String statementText
  ) {
    final UdfFactory udfFactory = executionContext.getMetaStore().getUdfFactory(functionName);

    final ImmutableList.Builder<FunctionInfo> listBuilder = ImmutableList.builder();

    udfFactory.eachFunction(func -> listBuilder.add(
        getFunctionInfo(
            func.getArguments(), func.getReturnType(), func.getDescription(), func.isVariadic())));

    return new FunctionDescriptionList(
        statementText,
        udfFactory.getName().toUpperCase(),
        udfFactory.getDescription(),
        udfFactory.getAuthor(),
        udfFactory.getVersion(),
        udfFactory.getPath(),
        listBuilder.build(),
        FunctionType.scalar
    );
  }

  private static FunctionInfo getFunctionInfo(
      final List<Schema> argTypes,
      final Schema returnTypeSchema,
      final String description,
      final boolean variadic) {
    final List<ArgumentInfo> args = new ArrayList<>();
    for (int i = 0; i < argTypes.size(); i++) {
      final Schema s = argTypes.get(i);
      final boolean isVariadic = variadic && i == (argTypes.size() - 1);
      final String sqlType = FORMATTER.format(isVariadic ? s.valueSchema() : s);
      args.add(new ArgumentInfo(s.name(), sqlType, s.doc(), isVariadic));
    }

    final String returnType = FORMATTER.format(returnTypeSchema);

    return new FunctionInfo(args, returnType, description);
  }

}
