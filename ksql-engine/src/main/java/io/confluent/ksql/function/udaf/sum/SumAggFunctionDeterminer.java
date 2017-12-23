/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function.udaf.sum;

import io.confluent.ksql.function.KsqlAggFunctionDeterminer;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.util.KsqlException;
import org.apache.kafka.connect.data.Schema;

import java.util.Arrays;
import java.util.List;

public class SumAggFunctionDeterminer extends KsqlAggFunctionDeterminer {

  public SumAggFunctionDeterminer() {
    super("SUM", Arrays.asList(new DoubleSumKudaf(-1), new LongSumKudaf(-1)));
  }

  @Override
  public KsqlAggregateFunction getProperAggregateFunction(List<Schema> argTypeList) {
    // For now we only support aggregate functions with one arg.
    for (KsqlAggregateFunction ksqlAggregateFunction : getAggregateFunctionList()) {
      if (ksqlAggregateFunction.hasSameArgTypes(argTypeList)) {
        return ksqlAggregateFunction;
      }
    }
    throw new KsqlException("No SUM aggregate function with " + argTypeList.get(0) + " "
                           + " argument type exists!");
  }

}
