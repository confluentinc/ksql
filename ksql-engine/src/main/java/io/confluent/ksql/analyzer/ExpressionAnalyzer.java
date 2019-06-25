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

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
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
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Searches through the AST for any column references and throws if they are unknown fields.
 */
class ExpressionAnalyzer {

  private final boolean isJoin;
  private final Map<String, LogicalSchema> dataSources;

  ExpressionAnalyzer(final List<AliasedDataSource> dataSources) {
    this.dataSources = ImmutableMap.copyOf(requireNonNull(dataSources, "dataSources")
        .stream()
        .collect(Collectors.toMap(
            AliasedDataSource::getAlias,
            s -> s.getDataSource().getSchema()
        )));

    this.isJoin = dataSources.size() > 1;
  }

  void analyzeExpression(final Expression expression) {
    final Visitor visitor = new Visitor();
    visitor.process(expression, null);
  }

  private void throwOnUnknownField(final String columnName) {

    final Optional<String> maybeAlias = SchemaUtil.getFieldNameAlias(columnName);
    if (maybeAlias.isPresent()) {
      final String alias = maybeAlias.get();
      final String fieldName = SchemaUtil.getFieldNameWithNoAlias(columnName);

      final LogicalSchema sourceSchema = dataSources.get(alias);
      if (sourceSchema == null) {
        throw new KsqlException("Unknown source name or alias: " + alias);
      }

      final boolean knownField = sourceSchema.findField(fieldName).isPresent();
      if (!knownField) {
        throw new KsqlException("Field '" + fieldName + "' not a field of source '" + alias + "'");
      }
    } else {
      final List<String> sourceWithField = dataSources.entrySet().stream()
          .filter(e -> e.getValue().findField(columnName).isPresent())
          .map(Entry::getKey)
          .map(alias -> SchemaUtil.buildAliasedFieldName(alias, columnName))
          .collect(Collectors.toList());

      if (sourceWithField.isEmpty()) {
        throw new KsqlException("Field '" + columnName + "' cannot be resolved.");
      }

      if (sourceWithField.size() > 1) {
        throw new KsqlException("Field '" + columnName + "' is ambiguous. "
            + "Could be any of: " + sourceWithField);
      }
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
      final String columnName = isJoin
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
