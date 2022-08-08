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
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.function.BiConsumer;
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
  public synchronized FunctionSource getFunction(final List<SqlType> argTypeList) {

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
    final int numInitArgs;
    final int numSignatureInitArgs = creator.literalParams().size();
    if (isFactoryVariadic) {
      numInitArgs = argTypeList.size() - (creator.parameterInfo().size() - numSignatureInitArgs);
    } else {
      numInitArgs = numSignatureInitArgs;
    }

    return new FunctionSource(
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
