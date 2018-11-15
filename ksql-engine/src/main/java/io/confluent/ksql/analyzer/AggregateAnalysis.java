/*
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

import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AggregateAnalysis {

  private final Set<DereferenceExpression> requiredColumns = new HashSet<>();
  private final Set<DereferenceExpression> groupByColumns = new HashSet<>();
  private final Set<DereferenceExpression> nonAggSelectColumns = new HashSet<>();
  private final Set<DereferenceExpression> nonAggHavingColumns = new HashSet<>();
  private final List<Expression> finalSelectExpressions = new ArrayList<>();
  private final List<Expression> aggregateFunctionArguments = new ArrayList<>();
  private final List<FunctionCall> functionList = new ArrayList<>();
  private Expression havingExpression = null;


  public List<Expression> getAggregateFunctionArguments() {
    return Collections.unmodifiableList(aggregateFunctionArguments);
  }

  /**
   * Get the full set of columns from the source schema that are required. This includes columns
   * used in SELECT, GROUP BY and HAVING clauses.
   *
   * @return the full set of columns from the source schema that are required.
   */
  public Set<DereferenceExpression> getRequiredColumns() {
    return Collections.unmodifiableSet(requiredColumns);
  }

  /**
   * Get the set of columns from the source schema that are used in the GROUP BY clause.
   *
   * @return the set of columns in the GROUP BY clause.
   */
  Set<DereferenceExpression> getGroupByColumns() {
    return Collections.unmodifiableSet(groupByColumns);
  }

  /**
   * Get the set of columns from the source schema that are using in the SELECT clause outside
   * of aggregate functions.
   *
   * @return the set of non-aggregate columns in the SELECT clause.
   */
  Set<DereferenceExpression> getNonAggregateSelectColumns() {
    return Collections.unmodifiableSet(nonAggSelectColumns);
  }

  /**
   * Get the set of columns from the source schema that are using in the HAVING clause outside
   * of aggregate functions.
   *
   * @return the set of non-aggregate columns in the HAVING clause.
   */
  Set<DereferenceExpression> getNonAggregateHavingColumns() {
    return Collections.unmodifiableSet(nonAggHavingColumns);
  }

  public List<FunctionCall> getFunctionList() {
    return Collections.unmodifiableList(functionList);
  }

  public List<Expression> getFinalSelectExpressions() {
    return Collections.unmodifiableList(finalSelectExpressions);
  }

  public Expression getHavingExpression() {
    return havingExpression;
  }

  void setHavingExpression(final Expression havingExpression) {
    this.havingExpression = havingExpression;
  }

  void addAggregateFunctionArgument(final Expression argument) {
    aggregateFunctionArguments.add(argument);
  }

  void addFunction(final FunctionCall functionCall) {
    functionList.add(functionCall);
  }

  void addGroupByColumn(final DereferenceExpression node) {
    groupByColumns.add(node);
  }

  void addNonAggregateSelectColumn(final DereferenceExpression node) {
    nonAggSelectColumns.add(node);
  }

  void addNonAggregateHavingColumn(final DereferenceExpression node) {
    nonAggHavingColumns.add(node);
  }

  void addRequiredColumn(final DereferenceExpression node) {
    requiredColumns.add(node);
  }

  void addFinalSelectExpression(final Expression expression) {
    finalSelectExpressions.add(expression);
  }
}
