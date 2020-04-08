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

import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.execution.util.ComparisonUtil;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.SourceSchemas;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;

/**
 * Validates types used in the where statement.
 */
public final class WhereTypeValidator {

  private final SourceSchemas sourceSchemas;
  private final FunctionRegistry functionRegistry;

  WhereTypeValidator(
      final SourceSchemas sourceSchemas,
      final FunctionRegistry functionRegistry
  ) {
    this.sourceSchemas = requireNonNull(sourceSchemas, "sourceSchemas");
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
  }

  /**
   * Validates the given where expression.
   */
  void validateWhereExpression(final Expression exp) {
    // Construct an expression type manager to be used when checking types.
    final ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(sourceSchemas,
        functionRegistry);

    // Traverse the AST and throw errors if necessary
    final TypeChecker typeChecker = new TypeChecker(expressionTypeManager);
    typeChecker.process(exp, null);
  }

  /**
   * Does the actual type checking, ensuring that the where expression uses the proper types and
   * then giving a decent error message if there's an issue.
   */
  private static final class TypeChecker extends VisitParentExpressionVisitor<Void, Object> {

    private final ExpressionTypeManager expressionTypeManager;

    TypeChecker(final ExpressionTypeManager expressionTypeManager) {
      this.expressionTypeManager = expressionTypeManager;
    }

    public Void visitExpression(final Expression exp, final Object context) {
      final SqlType type = expressionTypeManager.getExpressionSqlType(exp);
      if (!SqlTypes.BOOLEAN.equals(type)) {
        throw new KsqlException("Type error in WHERE expression: Should evaluate to "
            + "boolean but is " + exp.toString() + " (" + type.toString(FormatOptions.none())
            + ") instead.");
      }
      return null;
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression exp,
        final Object context
    ) {
      final SqlType leftType = expressionTypeManager.getExpressionSqlType(exp.getLeft());
      final SqlType rightType = expressionTypeManager.getExpressionSqlType(exp.getRight());

      if (!ComparisonUtil.isValidComparison(leftType, exp.getType(), rightType)) {
        throw new KsqlException("Type mismatch in WHERE expression: Cannot compare "
            + exp.getLeft().toString() + " (" + leftType.toString(FormatOptions.none()) + ") to "
            + exp.getRight().toString() + " (" + rightType.toString(FormatOptions.none()) + ").");
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
}
