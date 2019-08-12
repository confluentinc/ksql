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

package io.confluent.ksql.util;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter.Context;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.VisitParentExpressionVisitor;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class AggregateExpressionRewriter
    extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

  public static final String AGGREGATE_FUNCTION_VARIABLE_PREFIX = "KSQL_AGG_VARIABLE_";
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
    final String functionName = node.getName().getSuffix();
    if (functionRegistry.isAggregate(functionName)) {
      final String aggVarName = AGGREGATE_FUNCTION_VARIABLE_PREFIX + aggVariableIndex;
      aggVariableIndex++;
      return Optional.of(
          new QualifiedNameReference(node.getLocation(), QualifiedName.of(aggVarName)));
    } else {
      final List<Expression> arguments = new ArrayList<>();
      for (final Expression argExpression: node.getArguments()) {
        arguments.add(context.process(argExpression));
      }
      return Optional.of(new FunctionCall(node.getLocation(), node.getName(), arguments));
    }
  }
}
