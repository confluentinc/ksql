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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.function.UdtfUtil;
import io.confluent.ksql.execution.function.udtf.KudtfFlatMapper;
import io.confluent.ksql.execution.function.udtf.TableFunctionApplier;
import io.confluent.ksql.execution.plan.KStreamHolder;
import io.confluent.ksql.execution.plan.StreamFlatMap;
import io.confluent.ksql.execution.streams.transform.KsTransformer;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;

public final class StreamFlatMapBuilder {

  private StreamFlatMapBuilder() {
  }

  public static <K> KStreamHolder<K> build(
      final KStreamHolder<K> stream,
      final StreamFlatMap<K> step,
      final KsqlQueryBuilder queryBuilder
  ) {
    final List<FunctionCall> tableFunctions = step.getTableFunctions();
    final LogicalSchema schema = stream.getSchema();
    final Builder<TableFunctionApplier> tableFunctionAppliersBuilder = ImmutableList.builder();
    final CodeGenRunner codeGenRunner =
        new CodeGenRunner(schema, queryBuilder.getKsqlConfig(), queryBuilder.getFunctionRegistry());

    for (FunctionCall functionCall: tableFunctions) {
      final List<ExpressionMetadata> expressionMetadataList = new ArrayList<>(
          functionCall.getArguments().size());
      for (Expression expression : functionCall.getArguments()) {
        final ExpressionMetadata expressionMetadata =
            codeGenRunner.buildCodeGenFromParseTree(expression, "Table function");
        expressionMetadataList.add(expressionMetadata);
      }
      final KsqlTableFunction tableFunction = UdtfUtil.resolveTableFunction(
          queryBuilder.getFunctionRegistry(),
          functionCall,
          schema
      );
      final TableFunctionApplier tableFunctionApplier =
          new TableFunctionApplier(tableFunction, expressionMetadataList);
      tableFunctionAppliersBuilder.add(tableFunctionApplier);
    }

    final ImmutableList<TableFunctionApplier> tableFunctionAppliers = tableFunctionAppliersBuilder
        .build();

    final KStream<K, GenericRow> mapped = stream.getStream().flatTransformValues(
        () -> new KsTransformer<>(new KudtfFlatMapper<>(tableFunctionAppliers)),
        Named.as(StreamsUtil.buildOpName(step.getProperties().getQueryContext()))
    );

    return stream.withStream(
        mapped,
        buildSchema(
            stream.getSchema(),
            step.getTableFunctions(),
            queryBuilder.getFunctionRegistry()
        )
    );
  }

  public static LogicalSchema buildSchema(
      final LogicalSchema inputSchema,
      final List<FunctionCall> tableFunctions,
      final FunctionRegistry functionRegistry) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final List<Column> cols = inputSchema.value();

    // We copy all the original columns to the output schema
    schemaBuilder.keyColumns(inputSchema.key());
    for (Column col : cols) {
      schemaBuilder.valueColumn(col);
    }

    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        inputSchema, functionRegistry);

    // And add new columns representing the exploded values at the end
    for (int i = 0; i < tableFunctions.size(); i++) {
      final FunctionCall functionCall = tableFunctions.get(i);
      final ColumnName colName = ColumnName.synthesisedSchemaColumn(i);
      final SqlType fieldType = expressionTypeManager.getExpressionSqlType(functionCall);
      schemaBuilder.valueColumn(colName, fieldType);
    }

    return schemaBuilder.build();
  }
}