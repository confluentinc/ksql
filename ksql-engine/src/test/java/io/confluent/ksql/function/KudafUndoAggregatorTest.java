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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.udaf.KudafUndoAggregator;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.connect.data.Schema;
import org.junit.Test;

public class KudafUndoAggregatorTest {
  @Test
  public void shouldApplyUndoableAggregateFunctions() {
    final InternalFunctionRegistry functionRegistry = new InternalFunctionRegistry();
    final Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();
    aggValToValColumnMap.put(0, 1);
    aggValToValColumnMap.put(1, 0);
    final Map<Integer, TableAggregationFunction> aggValToAggFunctionMap = new HashMap<>();
    final KsqlAggregateFunction functionInfo = functionRegistry.getAggregate(
        "SUM", Schema.OPTIONAL_INT32_SCHEMA);
    assertThat(functionInfo, instanceOf(TableAggregationFunction.class));
    aggValToAggFunctionMap.put(
        2, (TableAggregationFunction)functionInfo.getInstance(
            new AggregateFunctionArguments(2, Collections.singletonList("baz"))
        ));

    final GenericRow row = new GenericRow(Arrays.asList("snow", "jon", 3));
    final GenericRow aggRow = new GenericRow(Arrays.asList("jon", "snow", 5));

    final KudafUndoAggregator aggregator = new KudafUndoAggregator(
        aggValToAggFunctionMap, aggValToValColumnMap);

    final GenericRow resultRow = aggregator.apply(null, row, aggRow);

    assertThat(resultRow, equalTo(new GenericRow(Arrays.asList("jon", "snow", 2))));
  }
}
