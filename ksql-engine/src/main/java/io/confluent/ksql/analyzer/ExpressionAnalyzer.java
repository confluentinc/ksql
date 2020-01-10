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
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.HashSet;
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

  Set<ColumnRef> analyzeExpression(
      final Expression expression,
      final boolean allowWindowMetaFields
  ) {
    final Set<ColumnRef> referencedColumns = new HashSet<>();
    final Visitor visitor = new Visitor(allowWindowMetaFields, referencedColumns);
    visitor.process(expression, null);
    return referencedColumns;
  }

  private final class Visitor extends VisitParentExpressionVisitor<Object, Object> {

    private final Set<ColumnRef> referencedColumns;
    private final boolean allowWindowMetaFields;

    Visitor(
        final boolean allowWindowMetaFields,
        final Set<ColumnRef> referencedColumns
    ) {
      this.allowWindowMetaFields = allowWindowMetaFields;
      this.referencedColumns = referencedColumns;
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
    public Object visitInListExpression(final InListExpression node, final Object context) {
      node.getValues().forEach(exp -> process(exp, null));
      return null;
    }

    @Override
    public Object visitSimpleCaseExpression(final SimpleCaseExpression node, final Object context) {
      process(node.getOperand(), null);
      node.getDefaultValue().ifPresent(exp -> process(exp, null));
      node.getWhenClauses().forEach(when -> {
        process(when.getOperand(), null);
        process(when.getResult(), null);
      });
      return null;
    }

    @Override
    public Object visitArithmeticUnary(final ArithmeticUnaryExpression node, final Object context) {
      process(node.getValue(), null);
      return null;
    }

    @Override
    public Object visitSearchedCaseExpression(
        final SearchedCaseExpression node,
        final Object context
    ) {
      node.getDefaultValue().ifPresent(exp -> process(exp, null));
      node.getWhenClauses().forEach(when -> {
        process(when.getOperand(), null);
        process(when.getResult(), null);
      });
      return null;
    }

    @Override
    public Object visitSubscriptExpression(final SubscriptExpression node, final Object context) {
      process(node.getBase(), null);
      process(node.getIndex(), null);
      return null;
    }

    @Override
    public Object visitStructExpression(final CreateStructExpression node, final Object context) {
      node.getFields().forEach(f -> process(f.getValue(), null));
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
      final ColumnRef reference = node.getReference();
      throwOnUnknownOrAmbiguousColumn(reference);
      referencedColumns.add(reference);
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
