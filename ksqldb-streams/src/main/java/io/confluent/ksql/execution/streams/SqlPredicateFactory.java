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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.execution.transform.sqlpredicate.SqlPredicate;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;

public interface SqlPredicateFactory {

  SqlPredicate create(
      Expression filterExpression,
      LogicalSchema schema,
      KsqlConfig ksqlConfig,
      FunctionRegistry functionRegistry
  );

  default SqlPredicate create(
      Expression filterExpression,
      ExpressionEvaluator expressionEvaluator
  ) {
    return new SqlPredicate(filterExpression, expressionEvaluator);
  }

}
