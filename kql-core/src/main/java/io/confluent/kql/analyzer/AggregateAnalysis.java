/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.analyzer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.FunctionCall;

public class AggregateAnalysis {

  List<Expression> aggregateFunctionArguments = new ArrayList<>();
  List<Expression> requiredColumnsList = new ArrayList<>();

  Expression havingExpression = null;

  Map<String, Expression> requiredColumnsMap = new HashMap<>();

  List<FunctionCall> functionList = new ArrayList<>();

  List<Expression> nonAggResultColumns = new ArrayList<>();

  List<Expression> finalSelectExpressions = new ArrayList<>();

  public List<Expression> getAggregateFunctionArguments() {
    return aggregateFunctionArguments;
  }

  public List<Expression> getRequiredColumnsList() {
    return requiredColumnsList;
  }

  public Map<String, Expression> getRequiredColumnsMap() {
    return requiredColumnsMap;
  }

  public List<FunctionCall> getFunctionList() {
    return functionList;
  }

  public List<Expression> getNonAggResultColumns() {
    return nonAggResultColumns;
  }

  public List<Expression> getFinalSelectExpressions() {
    return finalSelectExpressions;
  }

  public Expression getHavingExpression() {
    return havingExpression;
  }

  public void setHavingExpression(Expression havingExpression) {
    this.havingExpression = havingExpression;
  }
}
