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

package io.confluent.ksql.planner.plan;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.analyzer.TableFunctionAnalysis;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.function.UdtfUtil;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.ConnectToSqlTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.List;
import java.util.Objects;
import javax.annotation.concurrent.Immutable;

@Immutable
public class FlatMapNode extends PlanNode {

  private final PlanNode source;
  private final LogicalSchema schema;
  private final TableFunctionAnalysis tableFunctionAnalysis;

  public FlatMapNode(
      final PlanNodeId id,
      final PlanNode source,
      final LogicalSchema schema,
      final TableFunctionAnalysis tableFunctionAnalysis
  ) {
    super(id, source.getNodeOutputType());
    this.source = Objects.requireNonNull(source, "source");
    this.schema = schema;
    this.tableFunctionAnalysis = tableFunctionAnalysis;
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public KeyField getKeyField() {
    return source.getKeyField();
  }

  @Override
  public List<PlanNode> getSources() {
    return ImmutableList.of(source);
  }

  public PlanNode getSource() {
    return source;
  }

  @Override
  public <C, R> R accept(final PlanVisitor<C, R> visitor, final C context) {
    return visitor.visitFlatMap(this, context);
  }

  @Override
  protected int getPartitions(final KafkaTopicClient kafkaTopicClient) {
    return source.getPartitions(kafkaTopicClient);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId().toString());

    final LogicalSchema outputSchema = buildLogicalSchema(
        getSchema(),
        builder.getFunctionRegistry(),
        tableFunctionAnalysis
    );

    return getSource().buildStream(builder).flatMap(outputSchema,
        tableFunctionAnalysis, contextStacker);
  }

  private LogicalSchema buildLogicalSchema(
      final LogicalSchema inputSchema,
      final FunctionRegistry functionRegistry,
      final TableFunctionAnalysis tableFunctionAnalysis
  ) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final List<Column> cols = inputSchema.value();

    // We copy all the original columns to the output schema
    schemaBuilder.keyColumns(inputSchema.key());
    for (int i = 0; i < cols.size(); i++) {
      schemaBuilder.valueColumn(cols.get(i));
    }

    final ConnectToSqlTypeConverter converter = SchemaConverters.connectToSqlConverter();

    // And add new columns representing the exploded values at the end
    for (int i = 0; i < tableFunctionAnalysis.getTableFunctions().size(); i++) {
      final KsqlTableFunction tableFunction =
          UdtfUtil.resolveTableFunction(functionRegistry,
              tableFunctionAnalysis.getTableFunctions().get(i), inputSchema);
      final ColumnName colName = ColumnName.udtfColumn(i);
      final SqlType fieldType = converter.toSqlType(tableFunction.getReturnType());
      schemaBuilder.valueColumn(colName, fieldType);
    }

    return schemaBuilder.build();
  }
}
