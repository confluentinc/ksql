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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.StreamFlatMapBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A node in the logical plan which represents a flat map operation - transforming a single row into
 * zero or more rows.
 */
@Immutable
public class FlatMapNode extends PlanNode {

  private final PlanNode source;
  private final LogicalSchema outputSchema;
  private final ImmutableList<SelectExpression> finalSelectExpressions;
  private final ImmutableAnalysis analysis;
  private final FunctionRegistry functionRegistry;

  public FlatMapNode(
      final PlanNodeId id,
      final PlanNode source,
      final FunctionRegistry functionRegistry,
      final ImmutableAnalysis analysis
  ) {
    super(id, source.getNodeOutputType());
    this.source = Objects.requireNonNull(source, "source");
    this.analysis = Objects.requireNonNull(analysis);
    this.functionRegistry = functionRegistry;
    this.finalSelectExpressions = buildFinalSelectExpressions();
    outputSchema = StreamFlatMapBuilder.buildSchema(
        source.getSchema(),
        analysis.getTableFunctions(),
        functionRegistry
    );
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

  private ImmutableList<SelectExpression> buildFinalSelectExpressions() {
    final TableFunctionExpressionRewriter tableFunctionExpressionRewriter =
        new TableFunctionExpressionRewriter();

    final ImmutableList.Builder<SelectExpression> selectExpressions = ImmutableList.builder();
    for (final SelectExpression select : analysis.getSelectExpressions()) {
      final Expression exp = select.getExpression();
      selectExpressions.add(
          SelectExpression.of(
              select.getAlias(),
              ExpressionTreeRewriter.rewriteWith(
                  tableFunctionExpressionRewriter::process, exp)
          ));
    }
    return selectExpressions.build();
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
