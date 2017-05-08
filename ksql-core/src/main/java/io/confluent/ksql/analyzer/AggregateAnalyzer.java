/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.analyzer;

import io.confluent.ksql.function.KSQLFunctions;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.planner.DefaultTraversalVisitor;

public class AggregateAnalyzer extends DefaultTraversalVisitor<Node, AnalysisContext> {

  AggregateAnalysis aggregateAnalysis;
  MetaStore metaStore;

  boolean hasAggregateFunction = false;

  public boolean isHasAggregateFunction() {
    return hasAggregateFunction;
  }

  public void setHasAggregateFunction(boolean hasAggregateFunction) {
    this.hasAggregateFunction = hasAggregateFunction;
  }

  public AggregateAnalyzer(AggregateAnalysis aggregateAnalysis, MetaStore metaStore) {
    this.aggregateAnalysis = aggregateAnalysis;
    this.metaStore = metaStore;
  }

  @Override
  protected Node visitFunctionCall(final FunctionCall node, final AnalysisContext context) {
    String functionName = node.getName().getSuffix();
    if (KSQLFunctions.isAnAggregateFunction(functionName)) {
      aggregateAnalysis.aggregateFunctionArguments.add(node.getArguments().get(0));
      aggregateAnalysis.functionList.add(node);
      hasAggregateFunction = true;
    }

    for (Expression argExp: node.getArguments()) {
      process(argExp, context);
    }
    return null;
  }

  @Override
  protected Node visitDereferenceExpression(final DereferenceExpression node,
                                             final AnalysisContext context) {
    String name = node.toString();
    if (aggregateAnalysis.getRequiredColumnsMap().get(name) == null) {
      aggregateAnalysis.getRequiredColumnsList().add(node);
      aggregateAnalysis.getRequiredColumnsMap().put(name, node);
    }
    return null;
  }
}
