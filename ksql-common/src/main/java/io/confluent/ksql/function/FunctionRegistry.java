/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function;

import java.util.List;
import org.apache.kafka.connect.data.Schema;

public interface FunctionRegistry {

  UdfFactory getUdfFactory(String functionName);

  void addFunction(KsqlFunction ksqlFunction);

  boolean addFunctionFactory(UdfFactory factory);

  boolean isAggregate(String functionName);

  KsqlAggregateFunction getAggregate(String functionName,
                                     Schema argumentType);

  void addAggregateFunctionFactory(AggregateFunctionFactory aggregateFunctionFactory);

  FunctionRegistry copy();

  List<UdfFactory> listFunctions();

  AggregateFunctionFactory getAggregateFactory(String functionName);

  List<AggregateFunctionFactory> listAggregateFunctions();
}
