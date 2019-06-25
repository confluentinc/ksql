/*
 * Copyright 2018 Confluent Inc.
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

import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Set;

/**
 * Searches through the AST for any column references and throws if they are unknown fields.
 */
class ExpressionAnalyzer {

  private final SourceSchemas sourceSchemas;

  ExpressionAnalyzer(final SourceSchemas sourceSchemas) {
    this.sourceSchemas = Objects.requireNonNull(sourceSchemas, "sourceSchemas");
  }

  void analyzeExpression(final Expression expression) {
    final Visitor visitor = new Visitor();
    visitor.process(expression, null);
  }

  private void throwOnUnknownField(final String columnName) {
    final Set<String> sourcesWithField = sourceSchemas.sourcesWithField(columnName);
    if (sourcesWithField.isEmpty()) {
      throw new KsqlException("Field '" + columnName + "' cannot be resolved.");
    }

    if (sourcesWithField.size() > 1) {
      throw new KsqlException("Field '" + columnName + "' is ambiguous. "
          + "Could be any of: " + sourcesWithField);
    }
  }

  private class Visitor extends AstVisitor<Object, Object> {

    protected Object visitLikePredicate(final LikePredicate node, final Object context) {
      process(node.getValue(), null);
      return null;
    }

    protected Object visitFunctionCall(final FunctionCall node, final Object context) {
      for (final Expression argExpr : node.getArguments()) {
        process(argExpr, null);
      }
      return null;
    }

    protected Object visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    protected Object visitIsNotNullPredicate(final IsNotNullPredicate node, final Object context) {
      return process(node.getValue(), context);
    }

    protected Object visitIsNullPredicate(final IsNullPredicate node, final Object context) {
      return process(node.getValue(), context);
    }

    protected Object visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    protected Object visitComparisonExpression(
        final ComparisonExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    protected Object visitNotExpression(final NotExpression node, final Object context) {
      return process(node.getValue(), null);
    }

    @Override
    protected Object visitCast(final Cast node, final Object context) {
      process(node.getExpression(), context);
      return null;
    }

    @Override
    protected Object visitDereferenceExpression(
        final DereferenceExpression node,
        final Object context
    ) {
      final String columnName = sourceSchemas.isJoin()
          ? node.toString()
          : node.getFieldName();

      throwOnUnknownField(columnName);
      return null;
    }

    @Override
    protected Object visitQualifiedNameReference(
        final QualifiedNameReference node,
        final Object context
    ) {
      final String columnName = node.getName().getSuffix();
      throwOnUnknownField(columnName);
      return null;
    }
  }
}
