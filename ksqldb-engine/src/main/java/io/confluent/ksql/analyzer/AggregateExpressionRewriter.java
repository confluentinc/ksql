/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.analyzer;

import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.ColumnNames;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AggregateExpressionRewriter
    extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

  private int aggVariableIndex = 0;
  private final FunctionRegistry functionRegistry;

  public AggregateExpressionRewriter(final FunctionRegistry functionRegistry) {
    super(Optional.empty());
    this.functionRegistry = functionRegistry;
  }

  @Override
  public Optional<Expression> visitFunctionCall(
      final FunctionCall node,
      final ExpressionTreeRewriter.Context<Void> context) {
    final FunctionName functionName = node.getName();
    if (functionRegistry.isAggregate(functionName)) {
      final ColumnName aggVarName = ColumnNames.aggregateColumn(aggVariableIndex);
      aggVariableIndex++;
      return Optional.of(
          new UnqualifiedColumnReferenceExp(node.getLocation(), aggVarName));
    } else {
      final List<Expression> arguments = new ArrayList<>();
      for (final Expression argExpression: node.getArguments()) {
        arguments.add(context.process(argExpression));
      }
      return Optional.of(new FunctionCall(node.getLocation(), node.getName(), arguments));
    }
  }
}
