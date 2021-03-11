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

package io.confluent.ksql.function.udaf.topk;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import java.util.Collections;
import java.util.List;

public class TopKAggregateFunctionFactory extends AggregateFunctionFactory {

  private static final String NAME = "TOPK";

  private static final List<List<ParamType>> SUPPORTED_TYPES = ImmutableList
      .<List<ParamType>>builder()
      .add(ImmutableList.of(ParamTypes.INTEGER))
      .add(ImmutableList.of(ParamTypes.LONG))
      .add(ImmutableList.of(ParamTypes.DOUBLE))
      .add(ImmutableList.of(ParamTypes.STRING))
      .build();

  public TopKAggregateFunctionFactory() {
    super(NAME);
  }

  private static final AggregateFunctionInitArguments DEFAULT_INIT_ARGS =
      new AggregateFunctionInitArguments(0, 1);

  @Override
  public KsqlAggregateFunction createAggregateFunction(
      final List<SqlArgument> argumentType,
      final AggregateFunctionInitArguments initArgs
  ) {
    if (argumentType.isEmpty()) {
      throw new KsqlException("TOPK function should have two arguments.");
    }
    final int tkValFromArg = (Integer)(initArgs.arg(0));
    final SqlType argSchema = argumentType.get(0).getSqlTypeOrThrow();
    switch (argSchema.baseType()) {
      case INTEGER:
        return new TopkKudaf<>(
            NAME,
            initArgs.udafIndex(),
            tkValFromArg,
            SqlTypes.array(SqlTypes.INTEGER),
            Collections.singletonList(ParamTypes.INTEGER),
            Integer.class);
      case BIGINT:
        return new TopkKudaf<>(
            NAME,
            initArgs.udafIndex(),
            tkValFromArg,
            SqlTypes.array(SqlTypes.BIGINT),
            Collections.singletonList(ParamTypes.LONG),
            Long.class);
      case DOUBLE:
        return new TopkKudaf<>(
            NAME,
            initArgs.udafIndex(),
            tkValFromArg,
            SqlTypes.array(SqlTypes.DOUBLE),
            Collections.singletonList(ParamTypes.DOUBLE),
            Double.class);
      case STRING:
        return new TopkKudaf<>(
            NAME,
            initArgs.udafIndex(),
            tkValFromArg,
            SqlTypes.array(SqlTypes.STRING),
            Collections.singletonList(ParamTypes.STRING),
            String.class);
      default:
        throw new KsqlException("No TOPK aggregate function with " + argumentType.get(0)
            + " argument type exists!");
    }
  }

  @Override
  public List<List<ParamType>> supportedArgs() {
    return SUPPORTED_TYPES;
  }

  @Override
  public AggregateFunctionInitArguments getDefaultArguments() {
    return DEFAULT_INIT_ARGS;
  }
}
