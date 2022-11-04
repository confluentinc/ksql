/*
 * Copyright 2020 Confluent Inc.
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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.engine.rewrite.StatementRewriteForMagicPseudoTimestamp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.KsqlStatementException;

/**
 * Validates types used in filtering statements.
 */
public final class FilterTypeValidator {

  private final LogicalSchema schema;
  private final FunctionRegistry functionRegistry;
  private final FilterType filterType;

  public FilterTypeValidator(
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry,
      final FilterType filterType
  ) {
    this.schema = requireNonNull(schema, "schema");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.filterType = requireNonNull(filterType, "filterType");
  }

  /**
   * Validates the given filter expression.
   */
  public void validateFilterExpression(final Expression exp) {
    final SqlType type = getExpressionReturnType(exp);
    if (!SqlTypes.BOOLEAN.equals(type)) {
      throw new KsqlStatementException(
          "Type error in " + filterType.name() + " expression: "
          + "Should evaluate to boolean but is"
          + " (" + type.toString(FormatOptions.none()) + ") instead.",
          "Type error in " + filterType.name() + " expression: "
          + "Should evaluate to boolean but is " + exp.toString()
          + " (" + type.toString(FormatOptions.none()) + ") instead.",
          exp.toString()
      );
    }
  }

  private SqlType getExpressionReturnType(
      final Expression exp
  ) {
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema,
        functionRegistry);

    // Rewrite the expression with magic timestamps, so type checking can pass
    final Expression magicTimestampRewrite =
        new StatementRewriteForMagicPseudoTimestamp().rewrite(exp);

    try {
      return expressionTypeManager.getExpressionSqlType(magicTimestampRewrite);
    } catch (KsqlStatementException e) {
      throw new KsqlStatementException(
          "Error in " + filterType.name() + " expression",
          "Error in " + filterType.name() + " expression: " + e.getUnloggedMessage(),
          exp.toString()
      );
    } catch (KsqlException e) {
      throw new KsqlStatementException(
          "Error in " + filterType.name() + " expression",
          "Error in " + filterType.name() + " expression: " + e.getMessage(),
          exp.toString()
      );
    }
  }

  // The expression type being validated.
  public enum FilterType {
    WHERE,
    HAVING
  }
}
