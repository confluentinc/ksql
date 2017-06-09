/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.parser.rewrite;

import io.confluent.ksql.function.KsqlFunctions;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionRewriter;
import io.confluent.ksql.parser.tree.ExpressionTreeRewriter;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;

import java.util.ArrayList;
import java.util.List;

public class AggregateExpressionRewriter extends ExpressionRewriter<Void> {

  public final static String AGGREGATE_FUNCTION_VARIABLE_PREFIX = "KSQL_AGG_VARIABLE_";
  int aggVariableIndex = 0;

  @Override
  public Expression rewriteFunctionCall(FunctionCall node, Void context,
                                        ExpressionTreeRewriter<Void> treeRewriter) {
    String functionName = node.getName().getSuffix();
    if (KsqlFunctions.isAnAggregateFunction(functionName)) {
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
