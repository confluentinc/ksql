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
import java.util.Map;
import java.util.Set;

interface AggregateAnalysis extends AggregateAnalysisResult {

  /**
   * Get a map of non-aggregate select expression to the set of source schema fields the
   * expression uses.
   *
   * @return the map of select expression to the set of source schema fields.
   */
  Map<Expression, Set<DereferenceExpression>> getNonAggregateSelectExpressions();

  /**
   * Get the set of select fields that are involved in aggregate columns, but not as parameters
   * to the aggregate functions.
   *
   * @return the set of fields used in aggregate columns outside of aggregate function parameters.
   */
  Set<DereferenceExpression> getAggregateSelectFields();

  /**
   * Get the set of columns from the source schema that are used in the HAVING clause outside
   * of aggregate functions.
   *
   * @return the set of non-aggregate columns used in the HAVING clause.
   */
  Set<DereferenceExpression> getNonAggregateHavingFields();
}
