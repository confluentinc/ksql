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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.execution.streams.StreamFlatMapBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.ColumnNames;
import io.confluent.ksql.name.FunctionName;
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
  private final ImmutableList<FunctionCall> tableFunctions;

  public FlatMapNode(
      final PlanNodeId id,
      final PlanNode source,
      final FunctionRegistry functionRegistry,
      final ImmutableAnalysis analysis
  ) {
    super(
        id,
        source.getNodeOutputType(),
        buildSchema(source, functionRegistry, analysis),
        buildFinalSelectExpressions(functionRegistry, analysis)
    );
    this.source = Objects.requireNonNull(source, "source");
    this.tableFunctions = ImmutableList.copyOf(analysis.getTableFunctions());
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

    return getSource().buildStream(builder).flatMap(
        tableFunctions,
        contextStacker
    );
  }

  private static ImmutableList<SelectExpression> buildFinalSelectExpressions(
      final FunctionRegistry functionRegistry,
      final ImmutableAnalysis analysis
  ) {
    final TableFunctionExpressionRewriter tableFunctionExpressionRewriter =
        new TableFunctionExpressionRewriter(functionRegistry);

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

  private static class TableFunctionExpressionRewriter
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private final FunctionRegistry functionRegistry;
    private int variableIndex = 0;

    TableFunctionExpressionRewriter(
        final FunctionRegistry functionRegistry
    ) {
      super(Optional.empty());
      this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    }

    @Override
    public Optional<Expression> visitFunctionCall(
        final FunctionCall node,
        final Context<Void> context
    ) {
      final FunctionName functionName = node.getName();
      if (functionRegistry.isTableFunction(functionName)) {
        final ColumnName varName = ColumnNames.synthesisedSchemaColumn(variableIndex);
        variableIndex++;
        return Optional.of(
            new UnqualifiedColumnReferenceExp(node.getLocation(), varName)
        );
      } else {
        final List<Expression> arguments = new ArrayList<>();
        for (final Expression argExpression : node.getArguments()) {
          arguments.add(context.process(argExpression));
        }
        return Optional.of(new FunctionCall(node.getLocation(), node.getName(), arguments));
      }
    }
  }

  private static LogicalSchema buildSchema(
      final PlanNode source,
      final FunctionRegistry functionRegistry,
      final ImmutableAnalysis analysis
  ) {
    return StreamFlatMapBuilder.buildSchema(
        source.getSchema(),
        analysis.getTableFunctions(),
        functionRegistry
    );
  }
}
