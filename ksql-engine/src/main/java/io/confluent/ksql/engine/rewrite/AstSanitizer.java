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

package io.confluent.ksql.engine.rewrite;

import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.CreateStreamAsSelect;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.Query;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

/**
 * Validate and clean ASTs generated from externally supplied statements
 */
public final class AstSanitizer {
  private AstSanitizer() {
  }

  public static Statement sanitize(final Statement node, final MetaStore metaStore) {
    final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
    dataSourceExtractor.extractDataSources(node);

    final RewriterPlugin rewriterPlugin = new RewriterPlugin(metaStore, dataSourceExtractor);

    final ExpressionRewriterPlugin expressionRewriterPlugin =
        new ExpressionRewriterPlugin(metaStore, dataSourceExtractor);
    final BiFunction<Expression, Void, Expression> expressionRewriter =
        (e, v) -> ExpressionTreeRewriter.rewriteWith(expressionRewriterPlugin::process, e, v);

    return (Statement) new StatementRewriter<>(expressionRewriter, rewriterPlugin::process)
        .rewrite(node, null);
  }

  private static final class RewriterPlugin extends
      AstVisitor<Optional<AstNode>, StatementRewriter.Context<Void>> {
    final MetaStore metaStore;
    final DataSourceExtractor dataSourceExtractor;

    private int selectItemIndex = 0;

    RewriterPlugin(final MetaStore metaStore, final DataSourceExtractor dataSourceExtractor) {
      super(Optional.empty());
      this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
      this.dataSourceExtractor
          = Objects.requireNonNull(dataSourceExtractor, "dataSourceExtractor");
    }

    @Override
    protected Optional<AstNode> visitCreateStreamAsSelect(
        final CreateStreamAsSelect node,
        final StatementRewriter.Context<Void> ctx
    ) {
      return Optional.of(
          new CreateStreamAsSelect(
              node.getLocation(),
              node.getName(),
              (Query) ctx.process(node.getQuery()),
              node.isNotExists(),
              node.getProperties(),
              node.getPartitionByColumn()
          )
      );
    }

    @Override
    protected Optional<AstNode> visitInsertInto(
        final InsertInto node,
        final StatementRewriter.Context<Void> ctx
    ) {
      final DataSource<?> target = getSource(
          node.getTarget(),
          node.getLocation().map(
              l -> new NodeLocation(
                  l.getLineNumber(),
                  l.getColumnNumber() +  "INSERT INTO".length()
              )
          )
      );
      if (target.getDataSourceType() != DataSourceType.KSTREAM) {
        throw new KsqlException(
            "INSERT INTO can only be used to insert into a stream. "
                + target.getName().toString(FormatOptions.noEscape()) + " is a table.");
      }
      return Optional.of(
          new InsertInto(
              node.getLocation(),
              node.getTarget(),
              (Query) ctx.process(node.getQuery()),
              node.getPartitionByColumn()
          )
      );
    }

    @Override
    protected Optional<AstNode> visitSingleColumn(
        final SingleColumn singleColumn,
        final StatementRewriter.Context<Void> ctx
    ) {
      if (singleColumn.getAlias().isPresent()) {
        selectItemIndex++;
        return Optional.empty();
      }
      final ColumnName alias;
      final Expression expression = ctx.process(singleColumn.getExpression());
      if (expression instanceof ColumnReferenceExp) {
        final ColumnRef name = ((ColumnReferenceExp) expression).getReference();
        if (dataSourceExtractor.isJoin()
            && dataSourceExtractor.getCommonFieldNames().contains(name.name())) {
          alias = ColumnName.generatedJoinColumnAlias(name);
        } else {
          alias = name.name();
        }
      } else if (expression instanceof DereferenceExpression) {
        final DereferenceExpression dereferenceExp = (DereferenceExpression) expression;
        final String dereferenceExpressionString = dereferenceExp.toString();
        alias = ColumnName.of(replaceDotFieldRef(
            dereferenceExpressionString.substring(
                dereferenceExpressionString.indexOf(KsqlConstants.DOT) + 1)));
      } else {
        alias = ColumnName.generatedColumnAlias(selectItemIndex);
      }
      selectItemIndex++;
      return Optional.of(
          new SingleColumn(singleColumn.getLocation(), expression, Optional.of(alias))
      );
    }

    private static String replaceDotFieldRef(final String input) {
      return input
          .replace(KsqlConstants.DOT, "_")
          .replace(KsqlConstants.STRUCT_FIELD_REF, "__");
    }

    private DataSource<?> getSource(
        final SourceName name,
        final Optional<NodeLocation> location
    ) {
      final DataSource<?> source = metaStore.getSource(name);
      if (source == null) {
        throw new InvalidColumnReferenceException(location, name.name() + " does not exist.");
      }

      return source;
    }
  }

  private static final class ExpressionRewriterPlugin extends
      VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {
    final MetaStore metaStore;
    final DataSourceExtractor dataSourceExtractor;

    ExpressionRewriterPlugin(
        final MetaStore metaStore,
        final DataSourceExtractor dataSourceExtractor) {
      super(Optional.empty());
      this.metaStore = Objects.requireNonNull(metaStore, "metaStore");
      this.dataSourceExtractor
          = Objects.requireNonNull(dataSourceExtractor, "dataSourceExtractor");
    }

    @Override
    public Optional<Expression> visitColumnReference(
        final ColumnReferenceExp expression,
        final Context<Void> ctx) {
      final ColumnRef columnRef = expression.getReference();
      if (columnRef.source().isPresent()) {
        return Optional.empty();
      }
      try {
        final ColumnName columnName = columnRef.name();
        final SourceName sourceName = dataSourceExtractor.getAliasFor(columnName);
        return Optional.of(
            new ColumnReferenceExp(expression.getLocation(), ColumnRef.of(sourceName, columnName))
        );
      } catch (final KsqlException e) {
        throw new InvalidColumnReferenceException(expression.getLocation(), e.getMessage());
      }
    }
  }

  private static final class InvalidColumnReferenceException extends KsqlException {

    private InvalidColumnReferenceException(
        final Optional<NodeLocation> location,
        final String message
    ) {
      super(location.map(loc -> loc + ": ").orElse("") + message);
    }
  }
}
