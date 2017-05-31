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
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.planner.DefaultTraversalVisitor;
import io.confluent.ksql.util.SchemaUtil;

public class AggregateAnalyzer extends DefaultTraversalVisitor<Node, AnalysisContext> {

  AggregateAnalysis aggregateAnalysis;
  MetaStore metaStore;
  Analysis analysis;

  boolean hasAggregateFunction = false;

  public boolean isHasAggregateFunction() {
    return hasAggregateFunction;
  }

  public void setHasAggregateFunction(boolean hasAggregateFunction) {
    this.hasAggregateFunction = hasAggregateFunction;
  }

  public AggregateAnalyzer(AggregateAnalysis aggregateAnalysis, MetaStore metaStore, Analysis analysis) {
    this.aggregateAnalysis = aggregateAnalysis;
    this.metaStore = metaStore;
    this.analysis = analysis;
  }

  @Override
  protected Node visitFunctionCall(final FunctionCall node, final AnalysisContext context) {
    String functionName = node.getName().getSuffix();
    if (KSQLFunctions.isAnAggregateFunction(functionName)) {
      if (node.getArguments().isEmpty()) {
        Expression argExpression;
        if (analysis.getJoin() != null) {
          Expression baseExpression = new QualifiedNameReference(QualifiedName.of(analysis
                                                                                      .getJoin()
                                                                                      .getLeftAlias()));
          argExpression = new DereferenceExpression(baseExpression, SchemaUtil.ROWKEY_NAME);
        } else {
          Expression baseExpression = new QualifiedNameReference(QualifiedName.of(analysis
                                                                                      .getFromDataSources().get(0).getRight()));
          argExpression = new DereferenceExpression(baseExpression, SchemaUtil.ROWKEY_NAME);
        }
        aggregateAnalysis.aggregateFunctionArguments.add(argExpression);
      } else {
        aggregateAnalysis.aggregateFunctionArguments.add(node.getArguments().get(0));
      }
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
