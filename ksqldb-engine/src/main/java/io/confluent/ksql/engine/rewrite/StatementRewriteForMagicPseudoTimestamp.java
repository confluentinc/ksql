/*
 * Copyright 2021 Confluent Inc.
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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.SystemColumns;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class StatementRewriteForMagicPseudoTimestamp {

  private static final Set<ColumnName> SUPPORTED_COLUMNS = ImmutableSet.<ColumnName>builder()
      .addAll(SystemColumns.windowBoundsColumnNames())
      .add(SystemColumns.ROWTIME_NAME)
      .build();


  private final PartialStringToTimestampParser parser;

  public StatementRewriteForMagicPseudoTimestamp() {
    this(new PartialStringToTimestampParser());
  }

  @VisibleForTesting
  StatementRewriteForMagicPseudoTimestamp(final PartialStringToTimestampParser parser) {
    this.parser = Objects.requireNonNull(parser, "parser");
  }

  public Expression rewrite(final Expression expression) {
    return new ExpressionTreeRewriter<>(new OperatorPlugin()::process)
        .rewrite(expression, null);
  }

  private static boolean noRewriteRequired(final Expression expression) {
    return !expression.toString().contains("ROWTIME");
  }

  private final class OperatorPlugin
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    private OperatorPlugin() {
      super(Optional.empty());
    }

    @Override
    public Optional<Expression> visitBetweenPredicate(
        final BetweenPredicate node,
        final Context<Void> context
    ) {
      if (!supportedColumnRef(node.getValue())) {
        return Optional.empty();
      }

      final Optional<Expression> min = maybeRewriteTimestamp(node.getMin());
      final Optional<Expression> max = maybeRewriteTimestamp(node.getMax());
      if (!min.isPresent() && !max.isPresent()) {
        return Optional.empty();
      }

      return Optional.of(
          new BetweenPredicate(
              node.getLocation(),
              node.getValue(),
              min.orElse(node.getMin()),
              max.orElse(node.getMax())
          )
      );
    }

    @Override
    public Optional<Expression> visitComparisonExpression(
        final ComparisonExpression node,
        final Context<Void> context
    ) {
      if (supportedColumnRef(node.getLeft())) {
        final Optional<Expression> right = maybeRewriteTimestamp(node.getRight());
        return right.map(r -> new ComparisonExpression(
            node.getLocation(),
            node.getType(),
            node.getLeft(),
            r
        ));
      }

      if (supportedColumnRef(node.getRight())) {
        final Optional<Expression> left = maybeRewriteTimestamp(node.getLeft());
        return left.map(l -> new ComparisonExpression(
            node.getLocation(),
            node.getType(),
            l,
            node.getRight()
        ));
      }

      return Optional.empty();
    }
  }

  private Optional<Expression> maybeRewriteTimestamp(final Expression maybeTimestamp) {
    if (!(maybeTimestamp instanceof StringLiteral)) {
      return Optional.empty();
    }

    final String text = ((StringLiteral) maybeTimestamp).getValue();

    return Optional.of(new LongLiteral(parser.parse(text)));
  }

  private static boolean supportedColumnRef(final Expression maybeColumnRef) {
    if (!(maybeColumnRef instanceof ColumnReferenceExp)) {
      return false;
    }

    return SUPPORTED_COLUMNS.contains(((ColumnReferenceExp) maybeColumnRef).getColumnName());
  }
}