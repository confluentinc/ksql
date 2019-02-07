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

import io.confluent.ksql.function.udf.UdfMetadata;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;


public class TestFunctionRegistry implements MutableFunctionRegistry {
  private final Map<String, UdfFactory> udfs = new HashMap<>();
  private final Map<String, AggregateFunctionFactory> udafs = new HashMap<>();

  @Override
  public UdfFactory getUdfFactory(final String functionName) {
    return udfs.get(functionName);
  }

  @Override
  public void addFunction(final KsqlFunction ksqlFunction) {
    ensureFunctionFactory(new UdfFactory(
        ksqlFunction.getKudfClass(),
        new UdfMetadata(ksqlFunction.getFunctionName(),
            "",
            "",
            "",
            "", false)));
    final UdfFactory udfFactory = udfs.get(ksqlFunction.getFunctionName());
    udfFactory.addFunction(ksqlFunction);  }

  @Override
  public void ensureFunctionFactory(final UdfFactory factory) {
    udfs.putIfAbsent(factory.getName().toUpperCase(), factory);
  }

  @Override
  public boolean isAggregate(final String functionName) {
    return udafs.containsKey(functionName.toUpperCase());
  }

  @Override
  public KsqlAggregateFunction getAggregate(final String functionName,
                                            final Schema expressionType) {
    return udafs.get(functionName.toUpperCase()).getProperAggregateFunction(
        Collections.singletonList(expressionType));
  }

  @Override
  public void addAggregateFunctionFactory(final AggregateFunctionFactory aggregateFunctionFactory) {
    udafs.put(aggregateFunctionFactory.getName().toUpperCase(), aggregateFunctionFactory);
  }

  @Override
  public List<UdfFactory> listFunctions() {
    return new ArrayList<>(udfs.values());
  }

  @Override
  public AggregateFunctionFactory getAggregateFactory(final String functionName) {
    return udafs.get(functionName.toUpperCase());
  }

  @Override
  public List<AggregateFunctionFactory> listAggregateFunctions() {
    return new ArrayList<>(udafs.values());
  }
}
