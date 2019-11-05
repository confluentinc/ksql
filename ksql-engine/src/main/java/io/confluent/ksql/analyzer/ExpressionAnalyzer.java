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

import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Searches through the AST for any column references and throws if they are unknown or ambiguous.
 */
class ExpressionAnalyzer {

  private final SourceSchemas sourceSchemas;

  ExpressionAnalyzer(final SourceSchemas sourceSchemas) {
    this.sourceSchemas = Objects.requireNonNull(sourceSchemas, "sourceSchemas");
  }

  void analyzeExpression(final Expression expression, final boolean allowWindowMetaFields) {
    final Visitor visitor = new Visitor(allowWindowMetaFields);
    visitor.process(expression, null);
  }

  private final class Visitor extends VisitParentExpressionVisitor<Object, Object> {

    private final boolean allowWindowMetaFields;

    Visitor(final boolean allowWindowMetaFields) {
      this.allowWindowMetaFields = allowWindowMetaFields;
    }

    public Object visitLikePredicate(final LikePredicate node, final Object context) {
      process(node.getValue(), null);
      return null;
    }

    public Object visitFunctionCall(final FunctionCall node, final Object context) {
      for (final Expression argExpr : node.getArguments()) {
        process(argExpr, null);
      }
      return null;
    }

    public Object visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    public Object visitIsNotNullPredicate(final IsNotNullPredicate node, final Object context) {
      return process(node.getValue(), context);
    }

    public Object visitIsNullPredicate(final IsNullPredicate node, final Object context) {
      return process(node.getValue(), context);
    }

    public Object visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    public Object visitComparisonExpression(
        final ComparisonExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    public Object visitNotExpression(final NotExpression node, final Object context) {
      return process(node.getValue(), null);
    }

    @Override
    public Object visitCast(final Cast node, final Object context) {
      process(node.getExpression(), context);
      return null;
    }

    @Override
    public Object visitColumnReference(
        final ColumnReferenceExp node,
        final Object context
    ) {
      throwOnUnknownOrAmbiguousColumn(node.getReference());
      return null;
    }

    @Override
    public Object visitDereferenceExpression(
        final DereferenceExpression node,
        final Object context
    ) {
      process(node.getBase(), context);
      return null;
    }

    private void throwOnUnknownOrAmbiguousColumn(final ColumnRef name) {
      final Set<SourceName> sourcesWithField = sourceSchemas.sourcesWithField(name);

      if (sourcesWithField.isEmpty()) {
        if (allowWindowMetaFields && name.name().equals(SchemaUtil.WINDOWSTART_NAME)) {
          return;
        }

        throw new KsqlException("Column '" + name.toString(FormatOptions.noEscape())
            + "' cannot be resolved.");
      }

      if (sourcesWithField.size() > 1) {
        final String possibilities = sourcesWithField.stream()
            .map(source -> SchemaUtil.buildAliasedFieldName(source.name(), name.name().name()))
            .sorted()
            .collect(Collectors.joining(", "));

        throw new KsqlException("Column '" + name.name().name() + "' is ambiguous. "
            + "Could be any of: " + possibilities);
      }
    }
  }
}
