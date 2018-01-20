/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionRewriter;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;

import java.util.ArrayList;
import java.util.List;

public class AggregateExpressionRewriter extends ExpressionRewriter<Void> {

  public static final String AGGREGATE_FUNCTION_VARIABLE_PREFIX = "KSQL_AGG_VARIABLE_";
  private int aggVariableIndex = 0;
  private final FunctionRegistry functionRegistry;

  public AggregateExpressionRewriter(final FunctionRegistry functionRegistry) {
    this.functionRegistry = functionRegistry;
  }

  @Override
  public Expression rewriteFunctionCall(FunctionCall node, Void context,
                                        ExpressionTreeRewriter<Void> treeRewriter) {
    String functionName = node.getName().getSuffix();
    if (functionRegistry.isAnAggregateFunction(functionName)) {
      String aggVarName = AGGREGATE_FUNCTION_VARIABLE_PREFIX + aggVariableIndex;
      aggVariableIndex++;
      return new QualifiedNameReference(QualifiedName.of(aggVarName));
    } else {
      List<Expression> arguments = new ArrayList<>();
      for (Expression argExpression: node.getArguments()) {
        arguments.add(treeRewriter.rewrite(argExpression, context));
      }
      return new FunctionCall(node.getName(), arguments);
    }
  }
}
