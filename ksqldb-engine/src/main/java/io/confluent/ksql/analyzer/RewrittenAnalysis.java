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

import io.confluent.ksql.analyzer.Analysis.AliasedDataSource;
import io.confluent.ksql.analyzer.Analysis.Into;
import io.confluent.ksql.analyzer.Analysis.JoinInfo;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.WindowExpression;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * A wrapper for an {@link io.confluent.ksql.analyzer.ImmutableAnalysis} that rewrites all
 * expressions using a given rewriter plugin (as provided to
 * {@link io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter}). This is useful when
 * planning queries to allow the planner to transform expressions as needed it builds up the
 * transformations needed to execute the query.
 */
public class RewrittenAnalysis implements ImmutableAnalysis {

  private final ImmutableAnalysis original;
  private final BiFunction<Expression, Context<Void>, Optional<Expression>> rewriter;

  public RewrittenAnalysis(
      final ImmutableAnalysis original,
      final BiFunction<Expression, Context<Void>, Optional<Expression>> rewriter
  ) {
    this.original = Objects.requireNonNull(original, "original");
    this.rewriter = Objects.requireNonNull(rewriter, "rewriter");
  }

  public ImmutableAnalysis getOriginal() {
    return original;
  }

  @Override
  public List<FunctionCall> getTableFunctions() {
    return rewriteList(original.getTableFunctions());
  }

  @Override
  public List<SelectItem> getSelectItems() {
    return original.getSelectItems().stream()
        .map(si -> {
              if (!(si instanceof SingleColumn)) {
                return si;
              }

              final SingleColumn singleColumn = (SingleColumn) si;
              return new SingleColumn(
                  singleColumn.getLocation(),
                  rewrite(singleColumn.getExpression()),
                  singleColumn.getAlias()
              );
            }
        )
        .collect(Collectors.toList());
  }

  @Override
  public Optional<Expression> getWhereExpression() {
    return rewriteOptional(original.getWhereExpression());
  }

  @Override
  public Optional<Into> getInto() {
    return original.getInto();
  }

  @Override
  public Set<ColumnName> getSelectColumnNames() {
    return original.getSelectColumnNames().stream()
        .map(this::rewrite)
        .collect(Collectors.toSet());
  }

  @Override
  public Optional<Expression> getHavingExpression() {
    return rewriteOptional(original.getHavingExpression());
  }

  @Override
  public Optional<WindowExpression> getWindowExpression() {
    return original.getWindowExpression();
  }

  @Override
  public ColumnReferenceExp getDefaultArgument() {
    return rewrite(original.getDefaultArgument());
  }

  @Override
  public Optional<PartitionBy> getPartitionBy() {
    return original.getPartitionBy()
        .map(partitionBy -> new PartitionBy(
            partitionBy.getLocation(),
            rewrite(partitionBy.getExpression()),
            partitionBy.getAlias()
                .map(this::rewrite)
        ));
  }

  @Override
  public Optional<GroupBy> getGroupBy() {
    return original.getGroupBy()
        .map(groupBy -> new GroupBy(
            groupBy.getLocation(),
            rewriteList(groupBy.getGroupingExpressions()),
            groupBy.getAlias()
                .map(this::rewrite)
        ));
  }

  @Override
  public OptionalInt getLimitClause() {
    return original.getLimitClause();
  }

  @Override
  public List<JoinInfo> getJoin() {
    return original.getJoin().stream().map(
        j -> new JoinInfo(
            j.getJoinedSources(),
            rewrite(j.getLeftJoinExpression()),
            rewrite(j.getRightJoinExpression()),
            j.getType(),
            j.getWithinExpression()
        )
    ).collect(Collectors.toList());
  }

  @Override
  public boolean isJoin() {
    return original.isJoin();
  }

  @Override
  public List<AliasedDataSource> getAllDataSources() {
    return original.getAllDataSources();
  }

  @Override
  public CreateSourceAsProperties getProperties() {
    return original.getProperties();
  }

  @Override
  public SourceSchemas getFromSourceSchemas(final boolean postAggregate) {
    return original.getFromSourceSchemas(postAggregate);
  }

  @Override
  public AliasedDataSource getFrom() {
    return original.getFrom();
  }

  private <T extends Expression> Optional<T> rewriteOptional(final Optional<T> expression) {
    return expression.map(this::rewrite);
  }

  private <T extends Expression> List<T> rewriteList(final List<T> expressions) {
    return expressions.stream()
        .map(this::rewrite)
        .collect(Collectors.toList());
  }

  private <T extends Expression> T rewrite(final T expression) {
    return ExpressionTreeRewriter.rewriteWith(rewriter, expression);
  }

  private ColumnName rewrite(final ColumnName name) {
    final UnqualifiedColumnReferenceExp colRef = new UnqualifiedColumnReferenceExp(name);
    return ExpressionTreeRewriter.rewriteWith(rewriter, colRef).getColumnName();
  }
}
