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
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlPreconditions;
import java.util.List;
import org.apache.kafka.connect.data.Schema;

public class SumAggFunctionFactory extends AggregateFunctionFactory {
  private static final String FUNCTION_NAME = "SUM";

  public SumAggFunctionFactory() {
    super(FUNCTION_NAME);
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(final List<Schema> argTypeList) {
    KsqlPreconditions.checkArgument(
        argTypeList.size() == 1,
        "expected exactly one argument to aggregate MAX function");

    final Schema argSchema = argTypeList.get(0);
    switch (argSchema.type()) {
      case INT32:
        return new IntegerSumKudaf(FUNCTION_NAME, -1);
      case INT64:
        return new LongSumKudaf(FUNCTION_NAME, -1);
      case FLOAT64:
        return new DoubleSumKudaf(FUNCTION_NAME, -1);
      case BYTES:
        DecimalUtil.requireDecimal(argSchema);
        return new DecimalSumKudaf(FUNCTION_NAME, -1, argSchema);
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
