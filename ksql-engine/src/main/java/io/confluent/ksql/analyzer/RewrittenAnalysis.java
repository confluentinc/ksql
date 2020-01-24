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
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.plan.SelectExpression;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.serde.SerdeOption;
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
    this.rewriter = Objects.requireNonNull(rewriter ,"rewriter");
  }

  public ImmutableAnalysis getOriginal() {
    return original;
  }

  @Override
  public List<FunctionCall> getTableFunctions() {
    return rewriteList(original.getTableFunctions());
  }

  @Override
  public List<SelectExpression> getSelectExpressions() {
    return original.getSelectExpressions().stream()
        .map(e -> SelectExpression.of(
            e.getAlias(),
            ExpressionTreeRewriter.rewriteWith(rewriter, e.getExpression())))
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
  public Set<ColumnRef> getSelectColumnRefs() {
    return original.getSelectColumnRefs().stream()
        .map(UnqualifiedColumnReferenceExp::new)
        .map(r -> ExpressionTreeRewriter.rewriteWith(rewriter, r))
        .map(UnqualifiedColumnReferenceExp::getReference)
        .collect(Collectors.toSet());
  }

  @Override
  public List<Expression> getGroupByExpressions() {
    return rewriteList(original.getGroupByExpressions());
  }

  @Override
  public Optional<WindowExpression> getWindowExpression() {
    return original.getWindowExpression();
  }

  @Override
  public Optional<Expression> getPartitionBy() {
    return rewriteOptional(original.getPartitionBy());
  }

  @Override
  public OptionalInt getLimitClause() {
    return original.getLimitClause();
  }

  @Override
  public Optional<JoinInfo> getJoin() {
    return original.getJoin().map(
        j -> new JoinInfo(
            ExpressionTreeRewriter.rewriteWith(rewriter, j.getLeftJoinExpression()),
            ExpressionTreeRewriter.rewriteWith(rewriter, j.getRightJoinExpression()),
            j.getType(),
            j.getWithinExpression()
        )
    );
  }

  @Override
  public List<AliasedDataSource> getFromDataSources() {
    return original.getFromDataSources();
  }

  @Override
  public Set<SerdeOption> getSerdeOptions() {
    return original.getSerdeOptions();
  }

  @Override
  public CreateSourceAsProperties getProperties() {
    return original.getProperties();
  }

  @Override
  public SourceSchemas getFromSourceSchemas() {
    return original.getFromSourceSchemas();
  }

  private <T extends Expression> Optional<T> rewriteOptional(final Optional<T> expression) {
    return expression.map(e -> ExpressionTreeRewriter.rewriteWith(rewriter, e));
  }

  private <T extends Expression> List<T> rewriteList(final List<T> expressions) {
    return expressions.stream()
        .map(e -> ExpressionTreeRewriter.rewriteWith(rewriter, e))
        .collect(Collectors.toList());
  }
}
