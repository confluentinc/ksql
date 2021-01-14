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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionExpression;
import io.confluent.ksql.execution.expression.tree.LambdaLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.metastore.MetaStore;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metastore.model.DataSource.DataSourceType;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.tree.AstNode;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.InsertInto;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.Statement;
import io.confluent.ksql.schema.ksql.ColumnAliasGenerator;
import io.confluent.ksql.schema.ksql.ColumnNames;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.AmbiguousColumnException;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.UnknownSourceException;
import java.util.List;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * Validate and clean ASTs generated from externally supplied statements
 *
 * <p>Ensures the follow:
 * <ol>
 *   <li>INSERT INTO statements are inserting into a STREAM, not a TABLE.</li>
 *   <li>All source table and stream are known.</li>
 *   <li>No unqualified column references are ambiguous</li>
 *   <li>All single column select items have an alias set
 *   that ensures they are unique across all sources</li>
 *   <li>Lambda arguments don't overlap with column references</li>
 * </ol>
 */
public final class AstSanitizer {
  private AstSanitizer() {
  }

  public static Statement sanitize(final Statement node, final MetaStore metaStore) {
    final DataSourceExtractor dataSourceExtractor = new DataSourceExtractor(metaStore);
    dataSourceExtractor.extractDataSources(node);

    final RewriterPlugin rewriterPlugin =
        new RewriterPlugin(metaStore, dataSourceExtractor);

    final ExpressionRewriterPlugin expressionRewriterPlugin =
        new ExpressionRewriterPlugin(dataSourceExtractor);

    final BiFunction<Expression, Object, Expression> expressionRewriter =
        (e, v) -> ExpressionTreeRewriter.rewriteWith(expressionRewriterPlugin::process, e, v);

    return (Statement) new StatementRewriter<>(expressionRewriter, rewriterPlugin::process)
        .rewrite(node, null);
  }

  private static final class RewriterPlugin extends
      AstVisitor<Optional<AstNode>, StatementRewriter.Context<Object>> {

    private final MetaStore metaStore;
    private final DataSourceExtractor dataSourceExtractor;
    private final ColumnAliasGenerator aliasGenerator;

    RewriterPlugin(
        final MetaStore metaStore,
        final DataSourceExtractor dataSourceExtractor
    ) {
      super(Optional.empty());
      this.metaStore = requireNonNull(metaStore, "metaStore");
      this.dataSourceExtractor = requireNonNull(dataSourceExtractor, "dataSourceExtractor");
      this.aliasGenerator = ColumnNames.columnAliasGenerator(
          dataSourceExtractor.getAllSources().stream()
              .map(AliasedDataSource::getDataSource)
              .map(DataSource::getSchema)
      );
    }

    @Override
    protected Optional<AstNode> visitInsertInto(
        final InsertInto node,
        final StatementRewriter.Context<Object> ctx
    ) {
      final DataSource target = metaStore.getSource(node.getTarget());
      if (target == null) {
        final Optional<NodeLocation> targetLocation = node.getLocation()
            .map(
                l -> new NodeLocation(
                    l.getLineNumber(),
                    l.getColumnNumber() + "INSERT INTO".length()
                )
            );

        throw new UnknownSourceException(targetLocation, node.getTarget());
      }

      if (target.getDataSourceType() != DataSourceType.KSTREAM) {
        throw new KsqlException(
            "INSERT INTO can only be used to insert into a stream. "
                + target.getName().toString(FormatOptions.noEscape()) + " is a table.");
      }

      return Optional.empty();
    }

    @Override
    protected Optional<AstNode> visitSingleColumn(
        final SingleColumn singleColumn,
        final StatementRewriter.Context<Object> ctx
    ) {
      if (singleColumn.getAlias().isPresent()) {
        return Optional.empty();
      }

      final ColumnName alias;
      final Expression expression = ctx.process(singleColumn.getExpression());

      if (expression instanceof ColumnReferenceExp) {
        final Optional<SourceName> source = ((ColumnReferenceExp) expression).maybeQualifier();
        final ColumnName name = ((ColumnReferenceExp) expression).getColumnName();
        if (source.isPresent() && dataSourceExtractor.isClashingColumnName(name)) {
          alias = ColumnNames.generatedJoinColumnAlias(source.get(), name);
        } else {
          alias = name;
        }
      } else {
        alias = aliasGenerator.uniqueAliasFor(expression);
      }

      return Optional.of(
          new SingleColumn(singleColumn.getLocation(), expression, Optional.of(alias))
      );
    }
  }

  private static final class ExpressionRewriterPlugin extends
      VisitParentExpressionVisitor<Optional<Expression>, Object> {

    private final DataSourceExtractor dataSourceExtractor;

    ExpressionRewriterPlugin(final DataSourceExtractor dataSourceExtractor) {
      super(Optional.empty());
      this.dataSourceExtractor = requireNonNull(dataSourceExtractor, "dataSourceExtractor");
    }

    @Override
    public Optional<Expression> visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp expression,
        final Object ctx
    ) {
      final ColumnName columnName = expression.getColumnName();
      if (ctx != null && ctx instanceof Context) {
        final Context currentContext = (Context) ctx;
        if (currentContext.getContext() instanceof LambdaContext) {
          final LambdaContext lambdaContext =
              (LambdaContext) currentContext.getContext();
          if (lambdaContext.getLambdaArguments().size() > 0
              && lambdaContext.getLambdaArguments().contains(columnName.text())) {
            return Optional.of(new LambdaLiteral(columnName.text()));
          }
        }
      }
      final List<SourceName> sourceNames = dataSourceExtractor.getSourcesFor(columnName);

      if (sourceNames.size() > 1) {
        throw new AmbiguousColumnException(expression, sourceNames);
      }

      if (sourceNames.isEmpty()) {
        // Unknown column: handled later.
        return Optional.empty();
      }

      return Optional.of(
          new QualifiedColumnReferenceExp(
              expression.getLocation(),
              sourceNames.get(0),
              columnName
          )
      );
    }

    @Override
    public Optional<Expression> visitLambdaExpression(
        final LambdaFunctionExpression expression,
        final Object ctx
    ) {
      dataSourceExtractor.getAllSources().forEach(aliasedDataSource -> {
        for (String argument : expression.getArguments()) {
          if (aliasedDataSource.getDataSource().getSchema().columns().stream()
              .map(column -> column.name().text()).collect(Collectors.toList())
              .contains(argument)) {
            throw new KsqlException("Lambda argument can't be a column name.");
          }
        }
      });
      return visitExpression(expression, ctx);
    }
  }
}
