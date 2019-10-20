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

package io.confluent.ksql.execution.function.udtf;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.UdtfUtil;
import io.confluent.ksql.execution.plan.StreamFlatMap;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;
import org.apache.kafka.streams.kstream.ValueMapper;

public class KudtfFlatMapper implements ValueMapper<GenericRow, Iterable<GenericRow>> {

  private final StreamFlatMap streamFlatMap;

  public KudtfFlatMapper(final StreamFlatMap streamFlatMap) {
    this.streamFlatMap = streamFlatMap;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterable<GenericRow> apply(final GenericRow row) {

    /*
    TODO this currently assumes function expression is a simple column ref expression
     if we want to support more complex expressions we can use CodeGenRunner stuff
     to evaluate expressions in a similar way to how SELECTs are evaluated.
     */
    final List<FunctionCall> functionCalls = streamFlatMap.getFunctionCalls();
    final FunctionCall functionCall = functionCalls.get(0);
    final ColumnReferenceExp exp = (ColumnReferenceExp) functionCall.getArguments().get(0);
    final ColumnName columnName = exp.getReference().name();
    final ColumnRef ref = ColumnRef.withoutSource(columnName);
    final OptionalInt indexInInput = streamFlatMap.getInputSchema().valueColumnIndex(ref);
    if (!indexInInput.isPresent()) {
      throw new IllegalArgumentException("Can't find input column " + columnName);
    }

    // TODO we can cache all this (and above) so we don't look it up each time
    final List<Object> unexplodedValue = row.getColumnValue(indexInInput.getAsInt());
    final KsqlTableFunction tableFunction = UdtfUtil.resolveTableFunction(
        streamFlatMap.getFunctionRegistry(),
        functionCall,
        streamFlatMap.getInputSchema()
    );

    final List<Object> list = tableFunction.flatMap(unexplodedValue);

    // TODO please note that this is not an optimised final implementation
    final List<GenericRow> rows = new ArrayList<>();
    for (Object val : list) {
      final List<Object> arrayList = new ArrayList<>(row.getColumns());
      arrayList.add(val);
      // The exploded result columns always go at the end
      rows.add(new GenericRow(arrayList));
    }

    return rows;
  }
}
