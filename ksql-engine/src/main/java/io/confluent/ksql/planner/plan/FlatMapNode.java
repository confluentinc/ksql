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
import io.confluent.ksql.analyzer.Analysis;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.function.UdtfUtil;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.ConnectToSqlTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import org.apache.kafka.connect.data.Schema;

/**
 * A node in the logical plan which represents a flat map operation - transforming a single row into
 * zero or more rows.
 */
@Immutable
public class FlatMapNode extends PlanNode {

  private final PlanNode source;
  private final LogicalSchema outputSchema;
  private final List<SelectExpression> finalSelectExpressions;
  private final Analysis analysis;
  private final FunctionRegistry functionRegistry;

  public FlatMapNode(
      final PlanNodeId id,
      final PlanNode source,
      final FunctionRegistry functionRegistry,
      final Analysis analysis
  ) {
    super(id, source.getNodeOutputType());
    this.source = Objects.requireNonNull(source, "source");
    this.analysis = Objects.requireNonNull(analysis);
    this.functionRegistry = functionRegistry;
    this.finalSelectExpressions = buildFinalSelectExpressions();
    outputSchema = buildLogicalSchema(source.getSchema());
  }

  @Override
  public LogicalSchema getSchema() {
    return outputSchema;
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
  public List<SelectExpression> getSelectExpressions() {
    return finalSelectExpressions;
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

    return getSource().buildStream(builder).flatMap(
        outputSchema,
        analysis.getTableFunctions(),
        contextStacker
    );
  }

  private LogicalSchema buildLogicalSchema(final LogicalSchema inputSchema) {
    final LogicalSchema.Builder schemaBuilder = LogicalSchema.builder();
    final List<Column> cols = inputSchema.value();

    // We copy all the original columns to the output schema
    schemaBuilder.keyColumns(inputSchema.key());
    for (Column col : cols) {
      schemaBuilder.valueColumn(col);
    }

    final ConnectToSqlTypeConverter converter = SchemaConverters.connectToSqlConverter();

    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        inputSchema, functionRegistry);

    // And add new columns representing the exploded values at the end
    for (int i = 0; i < analysis.getTableFunctions().size(); i++) {
      final FunctionCall functionCall = analysis.getTableFunctions().get(i);
      final KsqlTableFunction tableFunction =
          UdtfUtil.resolveTableFunction(functionRegistry,
              functionCall, inputSchema
          );
      final ColumnName colName = ColumnName.synthesisedSchemaColumn(i);
      final List<Schema> argTypes = new ArrayList<>(functionCall.getArguments().size());
      for (Expression expression : functionCall.getArguments()) {
        final SqlType argType = expressionTypeManager.getExpressionSqlType(expression);
        argTypes.add(SchemaConverters.sqlToConnectConverter().toConnectSchema(argType));
      }
      final SqlType fieldType = converter.toSqlType(tableFunction.getReturnType(argTypes));
      schemaBuilder.valueColumn(colName, fieldType);
    }

    return schemaBuilder.build();
  }

  private List<SelectExpression> buildFinalSelectExpressions() {
    final TableFunctionExpressionRewriter tableFunctionExpressionRewriter =
        new TableFunctionExpressionRewriter();
    final List<SelectExpression> selectExpressions = new ArrayList<>();
    for (final SelectExpression select : analysis.getSelectExpressions()) {
      final Expression exp = select.getExpression();
      selectExpressions.add(
          SelectExpression.of(
              select.getAlias(),
              ExpressionTreeRewriter.rewriteWith(
                  tableFunctionExpressionRewriter::process, exp)
          ));
    }
    return selectExpressions;
  }

  private class TableFunctionExpressionRewriter
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private int variableIndex = 0;

    TableFunctionExpressionRewriter() {
      super(Optional.empty());
    }

    @Override
    public Optional<Expression> visitFunctionCall(
        final FunctionCall node,
        final Context<Void> context
    ) {
      final String functionName = node.getName().name();
      if (functionRegistry.isTableFunction(functionName)) {
        final ColumnName varName = ColumnName.synthesisedSchemaColumn(variableIndex);
        variableIndex++;
        return Optional.of(
            new ColumnReferenceExp(node.getLocation(), ColumnRef.of(Optional.empty(), varName)));
      } else {
        final List<Expression> arguments = new ArrayList<>();
        for (final Expression argExpression : node.getArguments()) {
          arguments.add(context.process(argExpression));
        }
        return Optional.of(new FunctionCall(node.getLocation(), node.getName(), arguments));
      }
    }
  }
}
