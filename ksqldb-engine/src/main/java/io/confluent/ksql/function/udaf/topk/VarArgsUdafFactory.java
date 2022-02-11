/*
 * Copyright 2022 Confluent Inc.
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
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import java.util.List;

public class VarArgsUdafFactory extends AggregateFunctionFactory {
  public static final String FUNCTION_NAME = "varargs_udaf";

  public VarArgsUdafFactory() {
    super(FUNCTION_NAME);
  }

  @Override
  public KsqlAggregateFunction<?, ?, ?> createAggregateFunction(
      final List<SqlArgument> argTypeList,
      final AggregateFunctionInitArguments initArgs) {
    // JNH: Fudging the types to get started.
    return new VarArgsUdaf(0, SqlTypes.STRING, SqlTypes.STRING);
  }

  @Override
  protected List<List<ParamType>> supportedArgs() {
    // anything is a supported type
    return ImmutableList.of(ImmutableList.of());
  }
}
