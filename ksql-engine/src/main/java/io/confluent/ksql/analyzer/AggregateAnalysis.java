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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AggregateAnalysis {

  private List<Expression> requiredColumnsList = new ArrayList<>();
  private Expression havingExpression = null;
  private Map<String, Expression> requiredColumnsMap = new HashMap<>();
  private List<Expression> nonAggResultColumns = new ArrayList<>();
  private List<Expression> finalSelectExpressions = new ArrayList<>();
  List<Expression> aggregateFunctionArguments = new ArrayList<>();
  List<FunctionCall> functionList = new ArrayList<>();


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
