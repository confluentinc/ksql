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
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.util.ComparisonUtil;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
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
    // Construct an expression type manager to be used when checking types.
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema,
        functionRegistry);

    // Rewrite the expression with magic timestamps, so type checking can pass
    final Expression magicTimestampRewrite =
        new StatementRewriteForMagicPseudoTimestamp().rewrite(exp);

    // Traverse the AST and throw errors if necessary
    final TypeChecker typeChecker = new TypeChecker(filterType, expressionTypeManager);
    typeChecker.process(magicTimestampRewrite, null);
  }

  /**
   * Does the actual type checking, ensuring that the where expression uses the proper types and
   * then giving a decent error message if there's an issue.
   */
  private static final class TypeChecker extends VisitParentExpressionVisitor<Void, Object> {

    private final FilterType filterType;
    private final ExpressionTypeManager expressionTypeManager;

    TypeChecker(final FilterType filterType, final ExpressionTypeManager expressionTypeManager) {
      this.filterType = filterType;
      this.expressionTypeManager = expressionTypeManager;
    }

    public Void visitExpression(final Expression exp, final Object context) {
      final SqlType type;
      try {
        type = expressionTypeManager.getExpressionSqlType(exp);
      } catch (KsqlException e) {
        throw new KsqlStatementException("Error in " + filterType.name() + " expression: " +
            e.getMessage(), exp.toString());
      }
      if (!SqlTypes.BOOLEAN.equals(type)) {
        throw new KsqlStatementException("Type error in " + filterType.name() + " expression: "
            + "Should evaluate to boolean but is " + exp.toString()
            + " (" + type.toString(FormatOptions.none()) + ") instead.",
            exp.toString());
      }
      return null;
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression exp,
        final Object context
    ) {
      final SqlType leftType;
      final SqlType rightType;
      try {
        leftType = expressionTypeManager.getExpressionSqlType(exp.getLeft());
        rightType = expressionTypeManager.getExpressionSqlType(exp.getRight());
      } catch (KsqlException e) {
        throw new KsqlStatementException("Error in " + filterType.name() + " expression: " +
            e.getMessage(), exp.toString());
      }
      if (!ComparisonUtil.isValidComparison(leftType, exp.getType(), rightType)) {
        throw new KsqlStatementException("Type mismatch in " + filterType.name() + " expression: "
            + "Cannot compare "
            + exp.getLeft().toString() + " (" + leftType.toString(FormatOptions.none()) + ") to "
            + exp.getRight().toString() + " (" + rightType.toString(FormatOptions.none()) + ").",
            exp.toString());
      }
      return null;
    }

    @Override
    public Void visitLogicalBinaryExpression(
        final LogicalBinaryExpression exp,
        final Object context) {
      process(exp.getLeft(), context);
      process(exp.getRight(), context);
      return null;
    }
  }

  public enum FilterType {
    WHERE,
    HAVING
  }
}
