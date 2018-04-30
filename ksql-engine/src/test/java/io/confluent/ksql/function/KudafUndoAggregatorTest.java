/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.function;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.hamcrest.CoreMatchers.instanceOf;

public class KudafUndoAggregatorTest {
  @Test
  public void shouldApplyUndoableAggregateFunctions() {
    Schema schema = SchemaBuilder
        .struct()
        .field("foo", SchemaBuilder.string().build())
        .field("bar", SchemaBuilder.string().build())
        .field("baz", SchemaBuilder.int32().build())
        .build();
    FunctionRegistry functionRegistry = new FunctionRegistry();
    Map<Integer, Integer> aggValToValColumnMap = new HashMap<>();
    aggValToValColumnMap.put(0, 1);
    aggValToValColumnMap.put(1, 0);
    Map<Integer, TableAggregationFunction> aggValToAggFunctionMap = new HashMap<>();
    Map<String, Integer> expressionNames = Collections.singletonMap("baz", 2);
    List<Expression> expressions = Collections.singletonList(
        new QualifiedNameReference(QualifiedName.of("baz")));
    KsqlAggregateFunction functionInfo = functionRegistry.getAggregateFunction(
        "SUM", expressions, schema);
    assertThat(functionInfo, instanceOf(TableAggregationFunction.class));
    aggValToAggFunctionMap.put(
        2, (TableAggregationFunction)functionInfo.getInstance(expressionNames, expressions));

    GenericRow row = new GenericRow(Arrays.asList("snow", "jon", 3));
    GenericRow aggRow = new GenericRow(Arrays.asList("jon", "snow", 5));

    KudafUndoAggregator aggregator = new KudafUndoAggregator(
        aggValToAggFunctionMap, aggValToValColumnMap);

    GenericRow resultRow = aggregator.apply("unused", row, aggRow);

    assertThat(resultRow, equalTo(new GenericRow(Arrays.asList("jon", "snow", 2))));
  }
}
