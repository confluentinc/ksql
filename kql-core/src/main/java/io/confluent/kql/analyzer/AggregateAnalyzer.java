/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.analyzer;

import io.confluent.kql.function.KQLFunctions;
import io.confluent.kql.metastore.MetaStore;
import io.confluent.kql.parser.tree.DereferenceExpression;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.FunctionCall;
import io.confluent.kql.parser.tree.Node;
import io.confluent.kql.planner.DefaultTraversalVisitor;

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
    if (KQLFunctions.getAggregateFunction(functionName) != null) {
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
    if (aggregateAnalysis.getRequiredColumnsMap().get(name.toUpperCase()) == null) {
      aggregateAnalysis.getRequiredColumnsList().add(node);
      aggregateAnalysis.getRequiredColumnsMap().put(name.toUpperCase(), node);
    }
    return null;
  }
}
