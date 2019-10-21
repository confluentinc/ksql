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

import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.plan.SelectExpression;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Encapsulates data that's been extracted from a query related to table functions.
 */
public class TableFunctionAnalysis {

  private final List<FunctionCall> tableFunctions = new ArrayList<>();
  private final List<SelectExpression> selectExpressions = new ArrayList<>();

  public List<FunctionCall> getTableFunctions() {
    return Collections.unmodifiableList(tableFunctions);
  }

  public List<SelectExpression> getFinalSelectExpressions() {
    return selectExpressions;
  }

  void addTableFunction(final FunctionCall functionCall) {
    tableFunctions.add(Objects.requireNonNull(functionCall));
  }

  void addFinalSelectExpression(final SelectExpression selectExpression) {
    this.selectExpressions.add(Objects.requireNonNull(selectExpression));
  }
}
