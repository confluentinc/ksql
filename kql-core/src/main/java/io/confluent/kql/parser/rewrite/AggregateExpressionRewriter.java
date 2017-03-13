package io.confluent.kql.parser.rewrite;


import java.util.ArrayList;
import java.util.List;

import io.confluent.kql.function.KQLFunctions;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.ExpressionRewriter;
import io.confluent.kql.parser.tree.ExpressionTreeRewriter;
import io.confluent.kql.parser.tree.FunctionCall;
import io.confluent.kql.parser.tree.QualifiedName;
import io.confluent.kql.parser.tree.QualifiedNameReference;

public class AggregateExpressionRewriter extends ExpressionRewriter<Void> {

  public final static String AGGREGATE_FUNCTION_VARIABLE_PREFIX = "KQL_AGG_VARIABLE_";
  int aggVariableIndex = 0;

  @Override
  public Expression rewriteFunctionCall(FunctionCall node, Void context,
                                        ExpressionTreeRewriter<Void> treeRewriter) {
    String functionName = node.getName().getSuffix();
//    if (functionName.equalsIgnoreCase("sum") || functionName.equalsIgnoreCase("count")) {
    if (KQLFunctions.getAggregateFunction(functionName) != null) {
      String aggVarName = AGGREGATE_FUNCTION_VARIABLE_PREFIX + aggVariableIndex;
      aggVariableIndex++;
      return new QualifiedNameReference(QualifiedName.of(aggVarName));
    } else {
//      return super.rewriteFunctionCall(node, context, treeRewriter);
//      return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter(), node);
      List<Expression> arguments = new ArrayList<>();
      for (Expression argExpression: node.getArguments()) {
        arguments.add(treeRewriter.rewrite(argExpression, context));
      }
      return new FunctionCall(node.getName(), arguments);
    }
  }
}
