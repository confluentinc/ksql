/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.schema.ksql;

import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.name.ColumnName;

/**
 * Generator of column names.
 *
 * <p>Used to generate unique aliases where the user has not supplied one, e.g. in SELECT, GROUP BY
 * and PARTITION BY clauses.
 */
public interface ColumnAliasGenerator {

  /**
   * Get a unique column aliases in the form {@code KSQL_COL_x}.
   *
   * <p>Used where the user hasn't specified an alias for an expression in a SELECT. This generated
   * column names are exposed to the user in the output schema.
   *
   * @return unique column alias
   */
  ColumnName nextKsqlColAlias();

  /**
   * Get a unique column name for the supplied {@code expression}.
   *
   * <p>Will return the same as {@link #nextKsqlColAlias()} for any expression type that does not
   * require special handling.
   *
   * <p>Special handling for {@link DereferenceExpression} returns a unique alias based on the
   * field name.
   *
   * @param expression the expression
   * @return unique column alias
   */
  ColumnName uniqueAliasFor(Expression expression);
}
