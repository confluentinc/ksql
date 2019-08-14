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

package io.confluent.ksql.parser.rewrite;

import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.parser.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.timestamp.StringToTimestampParser;

import java.time.ZoneId;
import java.util.Objects;
import java.util.Optional;

public class StatementRewriteForRowtime {
  private final Expression expression;

  public StatementRewriteForRowtime(final Expression expression) {
    this.expression = Objects.requireNonNull(expression, "expression");
  }

  public static boolean requiresRewrite(final Expression expression) {
    return expression.toString().contains("ROWTIME");
  }

  public Expression rewriteForRowtime() {
    return new ExpressionTreeRewriter<>(
        new OperatorPlugin()::process).rewrite(expression, null);
  }

  private static final class OperatorPlugin
      extends VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {
    private OperatorPlugin() {
      super(Optional.empty());
    }

    @Override
    public Optional<Expression> visitBetweenPredicate(
        final BetweenPredicate node,
        final Context<Void> context) {
      if (requiresRewrite(node.getValue())) {
        return Optional.of(
            new BetweenPredicate(
                node.getLocation(),
                node.getValue(),
                rewriteTimestamp(((StringLiteral) node.getMin()).getValue()),
                rewriteTimestamp(((StringLiteral) node.getMax()).getValue())
            )
        );
      }
      return Optional.empty();
    }

    @Override
    public Optional<Expression> visitComparisonExpression(
        final ComparisonExpression node,
        final Context<Void> context) {
      if (expressionIsRowtime(node.getLeft()) && node.getRight() instanceof StringLiteral) {
        return Optional.of(
            new ComparisonExpression(
                node.getLocation(),
                node.getType(),
                node.getLeft(),
                rewriteTimestamp(((StringLiteral) node.getRight()).getValue())
            )
        );
      } else if (expressionIsRowtime(node.getRight()) && node.getLeft() instanceof StringLiteral) {
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
    return (node instanceof DereferenceExpression)
        && ((DereferenceExpression) node).getFieldName().equals("ROWTIME");
  }

  private static LongLiteral rewriteTimestamp(final String timestamp) {
    final String timePattern = "HH:mm:ss.SSS";
    final StringToTimestampParser PARSER = new StringToTimestampParser(
        "yyyy-MM-dd'T'" + timePattern);

    final String date;
    final String time;
    final String timezone;

    if (timestamp.contains("T")) {
      date = timestamp.substring(0, timestamp.indexOf('T'));
      final String withTimezone = completeTime(timestamp.substring(timestamp.indexOf('T') + 1), timePattern);
      timezone = getTimezone(withTimezone);
      time = completeTime(withTimezone.substring(0, timezone.length()), timePattern);
    } else {
      date = completeDate(timestamp);
      time = completeTime("", timePattern);
      timezone = "";
    }

    try {
      if (timezone.length() > 0) {
        return new LongLiteral(PARSER.parse(date + "T" + time, ZoneId.of(timezone)));
      } else {
        return new LongLiteral(PARSER.parse(date + "T" + time));
      }
    } catch (final RuntimeException e) {
      throw new KsqlException("Failed to parse timestamp '"
          + timestamp + "': " + e.getMessage(), e);
    }
  }

  private static String getTimezone(final String time) {
    if (time.contains("+")) {
      return time.substring(time.indexOf('+'));
    } else if (time.contains("-")) {
      return time.substring(time.indexOf('-'));
    } else {
      return "";
    }
  }

  private static String completeDate(final String date) {
    final String[] parts = date.split("-");
    if (parts.length == 1) {
      return date + "-01-01";
    } else if (parts.length == 2) {
      return date + "-01";
    } else {
      // It is either a complete date or an incorrectly formatted one.
      // In the latter case, we can pass the incorrectly formed string
      // to the timestamp parser which will deal with the error handling.
      return date;
    }
  }

  private static String completeTime(final String time, final String timePattern) {
    if (time.length() >= timePattern.length()) {
      return time;
    }
    return time + timePattern.substring(time.length()).replaceAll("[a-zA-Z]", "0");
  }
}