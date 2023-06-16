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

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.types.DecimalType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;


public abstract class AggregateFunctionFactory {

  private final UdfMetadata metadata;

  // used in most numeric functions
  protected static final ImmutableList<List<ParamType>> NUMERICAL_ARGS = ImmutableList
      .<List<ParamType>>builder()
      .add(ImmutableList.of(ParamTypes.INTEGER))
      .add(ImmutableList.of(ParamTypes.LONG))
      .add(ImmutableList.of(ParamTypes.DOUBLE))
      .add(ImmutableList.of(ParamTypes.DECIMAL))
      .build();

  protected static final ImmutableList<List<ParamType>> NUMERICAL_TIME = ImmutableList
      .<List<ParamType>>builder().addAll(NUMERICAL_ARGS)
      .add(ImmutableList.of(ParamTypes.DATE))
      .add(ImmutableList.of(ParamTypes.TIME))
      .add(ImmutableList.of(ParamTypes.TIMESTAMP))
      .build();

  public AggregateFunctionFactory(final String functionName) {
    this(new UdfMetadata(
        functionName,
        "",
        KsqlConstants.CONFLUENT_AUTHOR,
        "",
        FunctionCategory.AGGREGATE,
        KsqlScalarFunction.INTERNAL_PATH
    ));
  }

  public AggregateFunctionFactory(final UdfMetadata metadata) {
    this.metadata = Objects.requireNonNull(metadata, "metadata can't be null");
  }

  public abstract KsqlAggregateFunction<?, ?, ?> createAggregateFunction(
      List<SqlArgument> argTypeList, AggregateFunctionInitArguments initArgs);

  protected abstract List<List<ParamType>> supportedArgs();

  public UdfMetadata getMetadata() {
    return metadata;
  }

  public String getName() {
    return metadata.getName();
  }

  public void eachFunction(final Consumer<KsqlAggregateFunction<?, ?, ?>> consumer) {
    supportedArgs()
        .stream()
        .map(args -> args
            .stream()
            .map(AggregateFunctionFactory::getSampleSqlType)
            .map(arg -> SqlArgument.of(arg))
            .collect(Collectors.toList()))
        .forEach(args -> consumer.accept(createAggregateFunction(args, getDefaultArguments())));
  }

  /**
   * This method turns a {@link ParamType} into a {@link SqlType} - this is a
   * narrowing operations in that the {@code SqlType} that is returned may represent
   * only a small subset of of what is a valid {@link ParamType}. For example,
   * the {@code DecimalType} will return a {@code SqlDecimal} with specific precision
   * and scale.
   */
  private static SqlType getSampleSqlType(final ParamType paramType) {
    if (paramType instanceof DecimalType) {
      return SqlDecimal.of(2, 1);
    }

    return SchemaConverters.functionToSqlConverter().toSqlType(paramType);
  }

  public AggregateFunctionInitArguments getDefaultArguments() {
    return AggregateFunctionInitArguments.EMPTY_ARGS;
  }

}
