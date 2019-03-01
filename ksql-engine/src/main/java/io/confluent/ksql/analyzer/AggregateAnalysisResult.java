/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.analyzer;

import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import java.util.List;

public interface AggregateAnalysisResult {

  List<Expression> getAggregateFunctionArguments();

  /**
   * Get the ordered list of columns from the source schema that are required. This includes columns
   * used in SELECT, GROUP BY and HAVING clauses.
   *
   * @return the full set of columns from the source schema that are required.
   */
  List<DereferenceExpression> getRequiredColumns();

  List<FunctionCall> getAggregateFunctions();

  List<Expression> getFinalSelectExpressions();

  Expression getHavingExpression();
}
