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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.util.SchemaUtil;
import io.confluent.ksql.util.timestamp.PartialStringToTimestampParser;
import java.util.Objects;
import java.util.Optional;

public class StatementRewriteForRowtime {

  private final PartialStringToTimestampParser parser;

  public StatementRewriteForRowtime() {
    this(new PartialStringToTimestampParser());
  }

  @VisibleForTesting
  StatementRewriteForRowtime(final PartialStringToTimestampParser parser) {
    this.parser = Objects.requireNonNull(parser, "parser");
  }

  public Expression rewriteForRowtime(final Expression expression) {
    if (noRewriteRequired(expression)) {
      return expression;
    }
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
      if (noRewriteRequired(node.getValue())) {
        return Optional.empty();
      }

      return Optional.of(
          new BetweenPredicate(
              node.getLocation(),
              node.getValue(),
              rewriteTimestamp(((StringLiteral) node.getMin()).getValue()),
              rewriteTimestamp(((StringLiteral) node.getMax()).getValue())
          )
      );
    }

    @Override
    public Optional<Expression> visitComparisonExpression(
        final ComparisonExpression node,
        final Context<Void> context
    ) {
      if (expressionIsRowtime(node.getLeft()) && node.getRight() instanceof StringLiteral) {
        return Optional.of(
            new ComparisonExpression(
                node.getLocation(),
                node.getType(),
                node.getLeft(),
                rewriteTimestamp(((StringLiteral) node.getRight()).getValue())
            )
        );
      }

      if (expressionIsRowtime(node.getRight()) && node.getLeft() instanceof StringLiteral) {
        return Optional.of(
            new ComparisonExpression(
                node.getLocation(),
                node.getType(),
                rewriteTimestamp(((StringLiteral) node.getLeft()).getValue()),
                node.getRight()
            )
        );
      }

      return Optional.empty();
    }
  }

  private static boolean expressionIsRowtime(final Expression node) {
    return (node instanceof ColumnReferenceExp)
        && ((ColumnReferenceExp) node).getReference().name().equals(SchemaUtil.ROWTIME_NAME);
  }

  private LongLiteral rewriteTimestamp(final String timestamp) {
    return new LongLiteral(parser.parse(timestamp));
  }
}