/*
 * Copyright 2018 Confluent Inc.
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

import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MutableAggregateAnalysis implements AggregateAnalysis {

  private final Set<DereferenceExpression> requiredColumns = new HashSet<>();
  private final Set<DereferenceExpression> groupByFields = new HashSet<>();
  private final Map<Expression, Set<DereferenceExpression>> nonAggSelectExpressions
      = new HashMap<>();
  private final Set<DereferenceExpression> nonAggHavingFields = new HashSet<>();
  private final List<Expression> finalSelectExpressions = new ArrayList<>();
  private final List<Expression> aggregateFunctionArguments = new ArrayList<>();
  private final List<FunctionCall> aggFunctions = new ArrayList<>();
  private Expression havingExpression = null;


  @Override
  public List<Expression> getAggregateFunctionArguments() {
    return Collections.unmodifiableList(aggregateFunctionArguments);
  }

  @Override
  public Set<DereferenceExpression> getRequiredColumns() {
    return Collections.unmodifiableSet(requiredColumns);
  }

  @Override
  public Set<DereferenceExpression> getGroupByFields() {
    return Collections.unmodifiableSet(groupByFields);
  }

  @Override
  public Map<Expression, Set<DereferenceExpression>> getNonAggregateSelectExpressions() {
    return Collections.unmodifiableMap(nonAggSelectExpressions);
  }

  @Override
  public Set<DereferenceExpression> getNonAggregateHavingFields() {
    return Collections.unmodifiableSet(nonAggHavingFields);
  }

  @Override
  public List<FunctionCall> getAggregateFunctions() {
    return Collections.unmodifiableList(aggFunctions);
  }

  @Override
  public List<Expression> getFinalSelectExpressions() {
    return Collections.unmodifiableList(finalSelectExpressions);
  }

  @Override
  public Expression getHavingExpression() {
    return havingExpression;
  }

  void setHavingExpression(final Expression havingExpression) {
    this.havingExpression = havingExpression;
  }

  void addAggregateFunctionArgument(final Expression argument) {
    aggregateFunctionArguments.add(argument);
  }

  void addAggFunction(final FunctionCall functionCall) {
    aggFunctions.add(functionCall);
  }

  void addGroupByField(final DereferenceExpression node) {
    groupByFields.add(node);
  }

  void addNonAggregateSelectExpression(
      final Expression selectExpression,
      final Set<DereferenceExpression> referencedFields
  ) {
    nonAggSelectExpressions.put(selectExpression, ImmutableSet.copyOf(referencedFields));
  }

  void addNonAggregateHavingField(final DereferenceExpression node) {
    nonAggHavingFields.add(node);
  }

  void addRequiredColumn(final DereferenceExpression node) {
    requiredColumns.add(node);
  }

  void addFinalSelectExpression(final Expression expression) {
    finalSelectExpressions.add(expression);
  }
}
