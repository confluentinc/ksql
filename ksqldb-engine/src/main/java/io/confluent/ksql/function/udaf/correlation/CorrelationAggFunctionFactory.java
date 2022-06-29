/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf.correlation;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.function.AggregateFunctionFactory;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;

public class CorrelationAggFunctionFactory extends AggregateFunctionFactory {
  private static final String FUNCTION_NAME = "CORRELATION";

  public CorrelationAggFunctionFactory() {
    super(FUNCTION_NAME);
  }

  @Override
  public KsqlAggregateFunction<?, ?, ?> createAggregateFunction(
          List<SqlArgument> argTypeList,
          AggregateFunctionInitArguments initArgs) {
    return new CorrelationKudaf(FUNCTION_NAME, initArgs.udafIndices());
  }

  @Override
  protected List<List<ParamType>> supportedArgs() {
    return ImmutableList.of(ImmutableList.of(ParamTypes.DOUBLE, ParamTypes.DOUBLE));
  }

  @Override
  public int numInitialArguments(List<SqlType> argumentTypes) {
    return 0;
  }
}
