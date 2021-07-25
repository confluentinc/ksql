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
import io.confluent.ksql.execution.windows.HoppingWindowExpression;
import io.confluent.ksql.execution.windows.KsqlWindowExpression;
import io.confluent.ksql.execution.windows.SessionWindowExpression;
import io.confluent.ksql.execution.windows.TumblingWindowExpression;
import io.confluent.ksql.execution.windows.WindowTimeClause;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.parser.NodeLocation;
import io.confluent.ksql.parser.OutputRefinement;
import io.confluent.ksql.parser.properties.with.CreateSourceAsProperties;
import io.confluent.ksql.parser.tree.GroupBy;
import io.confluent.ksql.parser.tree.PartitionBy;
import io.confluent.ksql.parser.tree.SelectItem;
import io.confluent.ksql.parser.tree.SingleColumn;
import io.confluent.ksql.parser.tree.WindowExpression;
import io.confluent.ksql.serde.RefinementInfo;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * A wrapper for an {@link io.confluent.ksql.analyzer.ImmutableAnalysis} that rewrites all
 * expressions using a given rewriter plugin (as provided to
 * {@link io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter}). This is useful when
 * planning queries to allow the planner to transform expressions as needed it builds up the
 * transformations needed to execute the query.
 */
// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class RewrittenAnalysis implements ImmutableAnalysis {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
  private final ImmutableAnalysis original;
  private final BiFunction<Expression, Context<Void>, Optional<Expression>> rewriter;
  static final WindowTimeClause zeroGracePeriod =
      new WindowTimeClause(0L, TimeUnit.MILLISECONDS);

  public RewrittenAnalysis(
      final ImmutableAnalysis original,
      final BiFunction<Expression, Context<Void>, Optional<Expression>> rewriter
  ) {
    this.original = Objects.requireNonNull(original, "original");
    this.rewriter = Objects.requireNonNull(rewriter, "rewriter");
  }

  public ImmutableAnalysis original() {
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
    final Optional<WindowExpression> windowExpression = original.getWindowExpression();
    final Optional<RefinementInfo> refinementInfo = original.getRefinementInfo();

    /* Return the original window expression, unless there is no grace period provided during a
    suppression, in which case we rewrite the window expression to have a default grace period
    of zero.
    */
    if (!(windowExpression.isPresent()
        && !windowExpression.get().getKsqlWindowExpression().getGracePeriod().isPresent()
        && refinementInfo.isPresent()
        && refinementInfo.get().getOutputRefinement() == OutputRefinement.FINAL
         )
    ) {
      return original.getWindowExpression();
    }
    final WindowExpression window = original.getWindowExpression().get();

    final KsqlWindowExpression ksqlWindowNew;
    final KsqlWindowExpression ksqlWindowOld = window.getKsqlWindowExpression();

    final Optional<NodeLocation> location = ksqlWindowOld.getLocation();
    final Optional<WindowTimeClause> retention = ksqlWindowOld.getRetention();

    if (ksqlWindowOld instanceof HoppingWindowExpression) {
      ksqlWindowNew = new HoppingWindowExpression(
          location,
          ((HoppingWindowExpression) ksqlWindowOld).getSize(),
          ((HoppingWindowExpression) ksqlWindowOld).getAdvanceBy(),
          retention,
          Optional.of(zeroGracePeriod)
      );
    } else if (ksqlWindowOld instanceof TumblingWindowExpression) {
      ksqlWindowNew = new TumblingWindowExpression(
          location,
          ((TumblingWindowExpression) ksqlWindowOld).getSize(),
          retention,
          Optional.of(zeroGracePeriod)
      );
    } else if (ksqlWindowOld instanceof SessionWindowExpression) {
      ksqlWindowNew = new SessionWindowExpression(
          location,
          ((SessionWindowExpression) ksqlWindowOld).getGap(),
          retention,
          Optional.of(zeroGracePeriod)
      );
    } else {
      throw new KsqlException("WINDOW type must be HOPPING, TUMBLING, or SESSION");
    }
    return Optional.of(new WindowExpression(
        original.getWindowExpression().get().getWindowName(),
        ksqlWindowNew
    ));
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
            rewriteList(partitionBy.getExpressions())
        ));
  }

  @Override
  public Optional<GroupBy> getGroupBy() {
    return original.getGroupBy()
        .map(groupBy -> new GroupBy(
            groupBy.getLocation(),
            rewriteList(groupBy.getGroupingExpressions())
        ));
  }

  @Override
  public Optional<RefinementInfo> getRefinementInfo() {
    return original.getRefinementInfo();
  }

  @Override
  public OptionalInt getLimitClause() {
    return original.getLimitClause();
  }

  @Override
  public List<JoinInfo> getJoin() {
    return original.getJoin();
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

  @Override
  public DataSource getDataSource() {
    return getFrom().getDataSource();
  }

  private <T extends Expression> Optional<T> rewriteOptional(final Optional<T> expression) {
    return expression.map(this::rewrite);
  }

  @Override
  public boolean getOrReplace() {
    return original.getOrReplace();
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
