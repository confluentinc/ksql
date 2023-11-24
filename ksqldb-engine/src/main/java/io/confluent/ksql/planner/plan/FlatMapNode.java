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
import io.confluent.ksql.analyzer.ImmutableAnalysis;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.context.QueryContext;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.streams.StreamFlatMapBuilder;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.structured.SchemaKStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * A node in the logical plan which represents a flat map operation - transforming a single row into
 * zero or more rows.
 */
public class FlatMapNode extends SingleSourcePlanNode {

  private final ImmutableList<FunctionCall> tableFunctions;
  private final ImmutableMap<Integer, Expression> columnMappings;
  private final LogicalSchema schema;

  public FlatMapNode(
      final PlanNodeId id,
      final PlanNode source,
      final FunctionRegistry functionRegistry,
      final ImmutableAnalysis analysis
  ) {
    super(
        id,
        source.getNodeOutputType(),
        Optional.empty(),
        source
    );
    this.schema = buildSchema(source, functionRegistry, analysis);
    this.tableFunctions = ImmutableList.copyOf(analysis.getTableFunctions());
    this.columnMappings = buildColumnMappings(functionRegistry, analysis);
  }

  @Override
  public LogicalSchema getSchema() {
    return schema;
  }

  @Override
  public Expression resolveSelect(final int idx, final Expression expression) {
    final Expression resolved = columnMappings.get(idx);
    return resolved == null ? expression : resolved;
  }

  @Override
  public SchemaKStream<?> buildStream(final PlanBuildContext buildContext) {

    final QueryContext.Stacker contextStacker = buildContext.buildNodeContext(getId().toString());

    return getSource().buildStream(buildContext).flatMap(
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

    final ImmutableMap.Builder<Integer, Expression> buildContext = ImmutableMap
        .builder();

    final List<SelectItem> selectItems = analysis.getSelectItems();
    for (int idx = 0; idx < selectItems.size(); idx++) {
      final SelectItem selectItem = selectItems.get(idx);
      if (!(selectItem instanceof SingleColumn)) {
        continue;
      }

      final SingleColumn singleColumn = (SingleColumn) selectItem;
      final Expression rewritten = ExpressionTreeRewriter.rewriteWith(
          tableFunctionExpressionRewriter::process, singleColumn.getExpression());

      if (!rewritten.equals(singleColumn.getExpression())) {
        buildContext.put(idx, rewritten);
      }
    }

    return buildContext.build();
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
