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
import io.confluent.ksql.function.types.GenericType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Function;
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
  public SqlType resolveReturnType(final List<SqlType> arguments) {
    final Pair<UdafFactoryInvoker, Map<GenericType, SqlType>> resolved =
            udfIndex.getFunctionAndGenerics(
                    arguments.stream()
                            .map(SqlArgument::of)
                            .collect(Collectors.toList())
            );

    return SchemaConverters.functionToSqlConverter()
            .toSqlType(resolved.getLeft().declaredReturnType(), resolved.getRight());
  }

  @Override
  public synchronized
      Pair<Integer, Function<AggregateFunctionInitArguments, KsqlAggregateFunction<?, ?, ?>>>
      getFunction(List<SqlType> argTypeList) {

    final List<SqlArgument> args = argTypeList.stream()
            .map((type) -> type == null ? null : SqlArgument.of(type))
            .collect(Collectors.toList());

    final UdafFactoryInvoker creator = udfIndex.getFunction(args);
    if (creator == null) {
      throw new KsqlException("There is no aggregate function with name='" + getName()
          + "' that has arguments of type="
          + argTypeList.stream()
          .map(SqlType::baseType)
          .map(Objects::toString)
          .collect(Collectors.joining(",")));
    }

    final boolean isFactoryVariadic = creator.literalParams().stream()
            .anyMatch(ParameterInfo::isVariadic);

    /* There can only be one variadic argument, so we know either the column args are bounded
    or the initial args are bounded. */
    int numInitArgs;
    if (isFactoryVariadic) {
      numInitArgs = argTypeList.size() - creator.parameterInfo().size();
    } else {
      numInitArgs = creator.literalParams().size();
    }

    return Pair.of(
            numInitArgs,
            (initArgs) -> creator.createFunction(initArgs, args)
    );
  }

  @Override
  public synchronized List<List<ParamType>> supportedArgs() {
    return udfIndex.values()
        .stream()
        .map(UdafFactoryInvoker::parameters)
        .collect(Collectors.toList());
  }

  @Override
  public void eachFunction(final BiConsumer<FunctionSignature, String> consumer) {
    udfIndex.values().forEach((invoker) -> consumer.accept(invoker, invoker.getDescription()));
  }

}
