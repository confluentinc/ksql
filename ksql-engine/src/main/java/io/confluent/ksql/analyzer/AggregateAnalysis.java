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

import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class AggregateAnalysis {

  private final Map<String, Expression> requiredColumnsMap = new LinkedHashMap<>();
  private final List<Expression> nonAggResultColumns = new ArrayList<>();
  private final List<Expression> finalSelectExpressions = new ArrayList<>();
  private final List<Expression> aggregateFunctionArguments = new ArrayList<>();
  private final List<FunctionCall> functionList = new ArrayList<>();
  private Expression havingExpression = null;


  public List<Expression> getAggregateFunctionArguments() {
    return Collections.unmodifiableList(aggregateFunctionArguments);
  }

  public List<Expression> getRequiredColumnsList() {
    return new ArrayList<>(requiredColumnsMap.values());
  }

  public Map<String, Expression> getRequiredColumnsMap() {
    return Collections.unmodifiableMap(requiredColumnsMap);
  }

  public List<FunctionCall> getFunctionList() {
    return Collections.unmodifiableList(functionList);
  }

  public List<Expression> getNonAggResultColumns() {
    return Collections.unmodifiableList(nonAggResultColumns);
  }

  public List<Expression> getFinalSelectExpressions() {
    return Collections.unmodifiableList(finalSelectExpressions);
  }

  public Expression getHavingExpression() {
    return havingExpression;
  }

  public void setHavingExpression(Expression havingExpression) {
    this.havingExpression = havingExpression;
  }

  public void addAggregateFunctionArgument(final Expression argument) {
    aggregateFunctionArguments.add(argument);
  }

  public void addFunction(final FunctionCall functionCall) {
    functionList.add(functionCall);
  }

  public boolean hasRequiredColumn(final String column) {
    return requiredColumnsMap.containsKey(column);
  }

  public void addRequiredColumn(final String name, final Expression node) {
    requiredColumnsMap.put(name, node);
  }

  public void addNonAggResultColumns(final Expression expression) {
    nonAggResultColumns.add(expression);
  }

  public void addFinalSelectExpression(final Expression expression) {
    finalSelectExpressions.add(expression);
  }
}
