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
    private static final String PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    @Override
    public Expression visitFunctionCall(final FunctionCall node, final Object context) {
      return (Expression) new StatementRewriter().process(node, context);
    }

    @Override
    public Node visitStringLiteral(final StringLiteral node, final Object context) {
      if (!node.getValue().equals("ROWTIME")) {
        final List<Expression> args = new ArrayList<>();
        args.add(new StringLiteral(appendZeros(node.getValue())));
        args.add(new StringLiteral(PATTERN));
        return new FunctionCall(QualifiedName.of("STRINGTOTIMESTAMP"), args);
      }
      return node;
    }

    private String appendZeros(final String timestamp) {
      if (timestamp.length() >= PATTERN.length()) {
        return timestamp;
      }
      return timestamp + PATTERN.substring(timestamp.length()).replaceAll("[a-zA-Z]", "0");
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
      if (StatementRewriteForRowtime.requiresRewrite(node)) {
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
}