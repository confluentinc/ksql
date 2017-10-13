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

package io.confluent.ksql.analyzer;

import io.confluent.ksql.function.KsqlFunctionRegistry;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.planner.DefaultTraversalVisitor;
import io.confluent.ksql.util.SchemaUtil;

public class AggregateAnalyzer extends DefaultTraversalVisitor<Node, AnalysisContext> {

  private AggregateAnalysis aggregateAnalysis;
  private Analysis analysis;
  private KsqlFunctionRegistry ksqlFunctionRegistry;

  private boolean hasAggregateFunction = false;

  public boolean isHasAggregateFunction() {
    return hasAggregateFunction;
  }

  public void setHasAggregateFunction(boolean hasAggregateFunction) {
    this.hasAggregateFunction = hasAggregateFunction;
  }

  public AggregateAnalyzer(AggregateAnalysis aggregateAnalysis,
                           Analysis analysis,
                           KsqlFunctionRegistry ksqlFunctionRegistry) {
    this.aggregateAnalysis = aggregateAnalysis;
    this.analysis = analysis;
    this.ksqlFunctionRegistry = ksqlFunctionRegistry;
  }

  @Override
  protected Node visitFunctionCall(final FunctionCall node, final AnalysisContext context) {
    String functionName = node.getName().getSuffix();
    if (ksqlFunctionRegistry.isAnAggregateFunction(functionName)) {
      if (node.getArguments().isEmpty()) {
        Expression argExpression;
        if (analysis.getJoin() != null) {
          Expression baseExpression = new QualifiedNameReference(
              QualifiedName.of(analysis.getJoin().getLeftAlias()));
          argExpression = new DereferenceExpression(baseExpression, SchemaUtil.ROWTIME_NAME);
        } else {
          Expression baseExpression = new QualifiedNameReference(
              QualifiedName.of(analysis.getFromDataSources().get(0).getRight()));
          argExpression = new DereferenceExpression(baseExpression, SchemaUtil.ROWTIME_NAME);
        }
        aggregateAnalysis.aggregateFunctionArguments.add(argExpression);
        node.getArguments().add(argExpression);
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
