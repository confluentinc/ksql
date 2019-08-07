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

import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.StringLiteral;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StatementRewriteForRowtime {
  private final Expression expression;

  public StatementRewriteForRowtime(final Expression expression) {
    this.expression = Objects.requireNonNull(expression, "expression");
  }

  public static boolean requiresRewrite(final Expression expression) {
    return expression.toString().contains("ROWTIME");
  }

  public Expression rewriteForRowtime() {
    return (Expression) new RewriteWithTimestampTransform().process(expression, null);
  }

  private static class TimestampRewriter extends StatementRewriter {
    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final String TIME_PATTERN = "HH:mm:ss.SSS";
    private static final String PATTERN = DATE_PATTERN + "'T'" + TIME_PATTERN;

    @Override
    public Expression visitFunctionCall(final FunctionCall node, final Object context) {
      return (Expression) new StatementRewriter().process(node, context);
    }

    @Override
    public Node visitStringLiteral(final StringLiteral node, final Object context) {
      if (!node.getValue().equals("ROWTIME")) {
        return new FunctionCall(
            QualifiedName.of("STRINGTOTIMESTAMP"),
            getFunctionArgs(node.getValue()));
      }
      return node;
    }

    private List<Expression> getFunctionArgs(final String datestring) {
      final List<Expression> args = new ArrayList<>();
      final String date;
      final String time;
      final String timezone;
      if (datestring.contains("T")) {
        date = datestring.substring(0, datestring.indexOf('T'));
        final String withTimezone = completeTime(datestring.substring(datestring.indexOf('T') + 1));
        timezone = getTimezone(withTimezone);
        time = completeTime(withTimezone.substring(0, timezone.length()));
      } else {
        date = completeDate(datestring);
        time = completeTime("");
        timezone = "";
      }

      if (timezone.length() > 0) {
        args.add(new StringLiteral(date + "T" + time));
        args.add(new StringLiteral(PATTERN));
        args.add(new StringLiteral(timezone));
      } else {
        args.add(new StringLiteral(date + "T" + time));
        args.add(new StringLiteral(PATTERN));
      }
      return args;
    }

    private String getTimezone(final String time) {
      if (time.contains("+")) {
        return time.substring(time.indexOf('+'));
      } else if (time.contains("-")) {
        return time.substring(time.indexOf('-'));
      } else {
        return "";
      }
    }

    private String completeDate(final String date) {
      final String[] parts = date.split("-");
      if (parts.length == 1) {
        return date + "-01-01";
      } else if (parts.length == 2) {
        return date + "-01";
      } else {
        // It is either a complete date or an incorrectly formatted one.
        // In the latter case, we can pass the incorrectly formed string
        // to STRINGTITIMESTAMP which will deal with the error handling.
        return date;
      }
    }

    private String completeTime(final String time) {
      if (time.length() >= TIME_PATTERN.length()) {
        return time;
      }
      return time + TIME_PATTERN.substring(time.length()).replaceAll("[a-zA-Z]", "0");
    }
  }

  private static class RewriteWithTimestampTransform extends StatementRewriter {
    @Override
    public Expression visitBetweenPredicate(final BetweenPredicate node, final Object context) {
      if (StatementRewriteForRowtime.requiresRewrite(node)) {
        return new BetweenPredicate(
            node.getLocation(),
            (Expression) new TimestampRewriter().process(node.getValue(), context),
            (Expression) new TimestampRewriter().process(node.getMin(), context),
            (Expression) new TimestampRewriter().process(node.getMax(), context));
      }
      return new BetweenPredicate(
          node.getLocation(),
          (Expression) process(node.getValue(), context),
          (Expression) process(node.getMin(), context),
          (Expression) process(node.getMax(), context));
    }

    @Override
    public Expression visitComparisonExpression(
        final ComparisonExpression node,
        final Object context) {
      if (expressionIsRowtime(node.getLeft()) || expressionIsRowtime(node.getRight())) {
        return new ComparisonExpression(
          node.getLocation(),
          node.getType(),
          (Expression) new TimestampRewriter().process(node.getLeft(), context),
          (Expression) new TimestampRewriter().process(node.getRight(), context));
      }
      return new ComparisonExpression(
          node.getLocation(),
          node.getType(),
          (Expression) process(node.getLeft(), context),
          (Expression) process(node.getRight(), context));
    }
  }

  private static boolean expressionIsRowtime(final Expression node) {
    return (node instanceof DereferenceExpression)
        && ((DereferenceExpression) node).getFieldName().equals("ROWTIME");
  }
}