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

package io.confluent.ksql.function.udaf.min;

import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlPreconditions;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class MinAggFunctionFactory extends AggregateFunctionFactory {

  private static final String FUNCTION_NAME = "MIN";

  public MinAggFunctionFactory() {
    super(FUNCTION_NAME);
  }

  @Override
  public KsqlAggregateFunction createAggregateFunction(
      final List<Schema> argTypeList,
      final AggregateFunctionInitArguments initArgs) {
    KsqlPreconditions.checkArgument(
        argTypeList.size() == 1,
        "expected exactly one argument to aggregate MAX function");

    final Schema argSchema = argTypeList.get(0);
    switch (argSchema.type()) {
      case INT32:
        return new IntegerMinKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case INT64:
        return new LongMinKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case FLOAT64:
        return new DoubleMinKudaf(FUNCTION_NAME, initArgs.udafIndex());
      case BYTES:
        DecimalUtil.requireDecimal(argSchema);
        return new DecimalMinKudaf(FUNCTION_NAME, initArgs.udafIndex(), argSchema);
      default:
        throw new KsqlException("No Max aggregate function with " + argTypeList.get(0) + " "
            + " argument type exists!");

    }
  }

  @Override
  public List<List<Schema>> supportedArgs() {
    return NUMERICAL_ARGS;
  }
}
