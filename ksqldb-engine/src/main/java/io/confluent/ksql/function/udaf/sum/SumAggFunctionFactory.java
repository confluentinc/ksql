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

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlPreconditions;
import java.util.List;

public class SumAggFunctionFactory extends AggregateFunctionFactory {

  private static final String FUNCTION_NAME = "SUM";

  public SumAggFunctionFactory() {
    super(FUNCTION_NAME);
  }

  @Override
  public KsqlAggregateFunction createAggregateFunction(
      final List<SqlArgument> argTypeList,
      final AggregateFunctionInitArguments initArgs
  ) {
    KsqlPreconditions.checkArgument(
        argTypeList.size() == 1,
        "expected exactly one argument to aggregate MAX function");

    final SqlType argSchema = argTypeList.get(0).getSqlType();
    switch (argSchema.baseType()) {
      case INTEGER:
        return new IntegerSumKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case BIGINT:
        return new LongSumKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case DOUBLE:
        return new DoubleSumKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case DECIMAL:
        return new DecimalSumKudaf(FUNCTION_NAME, initArgs.udafIndex(), (SqlDecimal) argSchema);
      default:
        throw new KsqlException("No Max aggregate function with " + argTypeList.get(0) + " "
            + " argument type exists!");

    }
  }

  @Override
  public List<List<ParamType>> supportedArgs() {
    return NUMERICAL_ARGS;
  }

}
