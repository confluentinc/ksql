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
import com.google.common.collect.ImmutableMap;
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
import io.confluent.ksql.execution.streams.StreamFlatMapBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.metastore.model.KeyField;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
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
  private final ImmutableMap<Integer, Expression> columnMappings;

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
        Optional.empty()
    );
    this.source = Objects.requireNonNull(source, "source");
    this.tableFunctions = ImmutableList.copyOf(analysis.getTableFunctions());
    this.columnMappings = buildColumnMappings(functionRegistry, analysis);
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
  public Expression resolveSelect(final int idx, final Expression expression) {
    return columnMappings.getOrDefault(idx, expression);
  }

  @Override
  public SchemaKStream<?> buildStream(final KsqlQueryBuilder builder) {

    final QueryContext.Stacker contextStacker = builder.buildNodeContext(getId().toString());

    return getSource().buildStream(builder).flatMap(
        tableFunctions,
        contextStacker
    );
  }

  private static ImmutableMap<Integer, Expression> buildColumnMappings(
      final FunctionRegistry functionRegistry,
      final ImmutableAnalysis analysis
  ) {
    final TableFunctionExpressionRewriter tableFunctionExpressionRewriter =
        new TableFunctionExpressionRewriter(functionRegistry);

    final ImmutableMap.Builder<Integer, Expression> builder = ImmutableMap.builder();

    for (int idx = 0; idx < analysis.getSelectItems().size(); idx++) {
      final SelectItem selectItem = analysis.getSelectItems().get(idx);
      if (!(selectItem instanceof SingleColumn)) {
        continue;
      }

      final SingleColumn singleColumn = (SingleColumn) selectItem;
      final Expression rewritten = ExpressionTreeRewriter.rewriteWith(
          tableFunctionExpressionRewriter::process, singleColumn.getExpression());

      if (!rewritten.equals(singleColumn.getExpression())) {
        builder.put(idx, rewritten);
      }
    }

    return builder.build();
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
        final ColumnName varName = ColumnName.synthesisedSchemaColumn(variableIndex);
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
