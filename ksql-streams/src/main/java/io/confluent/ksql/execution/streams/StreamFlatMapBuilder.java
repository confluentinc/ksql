/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.UdtfUtil;
import io.confluent.ksql.execution.function.udtf.KudtfFlatMapper;
import io.confluent.ksql.execution.function.udtf.TableFunctionApplier;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamFlatMap;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalInt;

public final class StreamFlatMapBuilder {

  private StreamFlatMapBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamFlatMap<K> step,
      final KsqlQueryBuilder queryBuilder) {
    final List<TableFunctionApplier> tableFunctionAppliers = new ArrayList<>();
    final LogicalSchema schema = step.getSource().getSchema();
    for (final FunctionCall functionCall : step.getTableFunctions()) {
      final ColumnReferenceExp exp = (ColumnReferenceExp)functionCall.getArguments().get(0);
      final ColumnName columnName = exp.getReference().name();
      final ColumnRef ref = ColumnRef.withoutSource(columnName);
      final OptionalInt indexInInput = schema.valueColumnIndex(ref);
      if (!indexInInput.isPresent()) {
        throw new IllegalArgumentException("Can't find input column " + columnName);
      }
      final KsqlTableFunction tableFunction = UdtfUtil.resolveTableFunction(
          queryBuilder.getFunctionRegistry(),
          functionCall,
          schema
      );
      final TableFunctionApplier tableFunctionApplier =
          new TableFunctionApplier(tableFunction, indexInInput.getAsInt());
      tableFunctionAppliers.add(tableFunctionApplier);
    }
    return stream.withStream(stream.getStream().flatMapValues(
        new KudtfFlatMapper(tableFunctionAppliers)));
  }

}