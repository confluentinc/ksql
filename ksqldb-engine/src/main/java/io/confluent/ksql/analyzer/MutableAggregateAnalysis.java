/*
 * Copyright 2021 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.analyzer;

import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public class MutableAggregateAnalysis implements AggregateAnalysisResult {

  private final List<ColumnReferenceExp> requiredColumns = new ArrayList<>();
  private final List<Expression> finalSelectExpressions = new ArrayList<>();
  private final List<Expression> aggregateFunctionArguments = new ArrayList<>();
  private final List<FunctionCall> aggFunctions = new ArrayList<>();
  private Optional<Expression> havingExpression = Optional.empty();

  void addAggregateFunctionArgument(final Expression argument) {
    aggregateFunctionArguments.add(argument);
  }

  @Override
  public List<Expression> getAggregateFunctionArguments() {
    return Collections.unmodifiableList(aggregateFunctionArguments);
  }

  void addRequiredColumn(final ColumnReferenceExp node) {
    if (!requiredColumns.contains(node)) {
      requiredColumns.add(node);
    }
  }

  @Override
  public List<ColumnReferenceExp> getRequiredColumns() {
    return Collections.unmodifiableList(requiredColumns);
  }

  void addAggFunction(final FunctionCall functionCall) {
    aggFunctions.add(functionCall);
  }

  @Override
  public List<FunctionCall> getAggregateFunctions() {
    return Collections.unmodifiableList(aggFunctions);
  }

  void addFinalSelectExpression(final Expression expression) {
    finalSelectExpressions.add(expression);
  }

  @Override
  public List<Expression> getFinalSelectExpressions() {
    return Collections.unmodifiableList(finalSelectExpressions);
  }

  void setHavingExpression(final Expression havingExpression) {
    this.havingExpression = Optional.of(havingExpression);
  }

  @Override
  public Optional<Expression> getHavingExpression() {
    return havingExpression;
  }
}
