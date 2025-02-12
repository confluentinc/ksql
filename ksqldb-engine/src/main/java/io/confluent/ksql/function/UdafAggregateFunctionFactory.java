/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlPrimitiveType;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class UdafAggregateFunctionFactory extends AggregateFunctionFactory {

  private final UdfIndex<UdafFactoryInvoker> udfIndex;

  UdafAggregateFunctionFactory(
      final UdfMetadata metadata,
      final List<UdafFactoryInvoker> factoryList
  ) {
    this(metadata, new UdfIndex<>(metadata.getName(), false));
    factoryList.forEach(udfIndex::addFunction);
  }

  @VisibleForTesting
  UdafAggregateFunctionFactory(
      final UdfMetadata metadata,
      final UdfIndex<UdafFactoryInvoker> index
  ) {
    super(metadata);
    udfIndex = Objects.requireNonNull(index);
  }

  @Override
  public synchronized KsqlAggregateFunction<?, ?, ?> createAggregateFunction(
      final List<SqlArgument> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    final List<SqlArgument> allParams = buildAllParams(argTypeList, initArgs);
    final UdafFactoryInvoker creator = udfIndex.getFunction(allParams);
    if (creator == null) {
      throw new KsqlException("There is no aggregate function with name='" + getName()
          + "' that has arguments of type="
          + allParams.stream()
          .map(SqlArgument::getSqlTypeOrThrow)
          .map(SqlType::baseType)
          .map(Objects::toString)
          .collect(Collectors.joining(",")));
    }
    return creator.createFunction(initArgs, argTypeList);
  }

  @Override
  public synchronized List<List<ParamType>> supportedArgs() {
    return udfIndex.values()
        .stream()
        .map(UdafFactoryInvoker::parameters)
        .collect(Collectors.toList());
  }

  private List<SqlArgument> buildAllParams(
      final List<SqlArgument> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    if (initArgs.args().isEmpty()) {
      return argTypeList;
    }

    final List<SqlArgument> allParams =
        new ArrayList<>(argTypeList.size() + initArgs.args().size());
    allParams.addAll(argTypeList);

    for (final Object arg : initArgs.args()) {
      if (arg == null) {
        allParams.add(null);
        continue;
      }

      final SqlBaseType baseType = SchemaConverters.javaToSqlConverter().toSqlType(arg.getClass());

      try {
        // Only primitive types currently supported:
        final SqlPrimitiveType primitiveType = SqlPrimitiveType.of(baseType);
        allParams.add(SqlArgument.of(primitiveType));
      } catch (final Exception e) {
        throw new KsqlFunctionException("Only primitive init arguments are supported by UDAF "
            + getName() + ", but got " + arg, e);
      }
    }

    return allParams;
  }
}
