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

import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableFunctionAnalysis {

  private final List<FunctionCall> tableFunctions = new ArrayList<>();
  private final List<Expression> selectExpressions = new ArrayList<>();
  private final List<ColumnReferenceExp> columns = new ArrayList<>();

  public List<FunctionCall> getTableFunctions() {
    return Collections.unmodifiableList(tableFunctions);
  }

  void addTableFunction(final FunctionCall functionCall) {
    tableFunctions.add(functionCall);
  }

  public List<Expression> getFinalSelectExpressions() {
    return selectExpressions;
  }

  void addFinalSelectExpression(final Expression selectExpression) {
    this.selectExpressions.add(selectExpression);
  }

  public List<ColumnReferenceExp> getColumns() {
    return columns;
  }

  void addColumn(final ColumnReferenceExp column) {
    this.columns.add(column);
  }

}
