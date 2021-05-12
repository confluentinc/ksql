/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.planner.plan;

import com.google.common.collect.Lists;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.engine.rewrite.StatementRewriteForMagicPseudoTimestamp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import java.util.Optional;

public final class PullQueryRewriter {

  private PullQueryRewriter() { }

  public static Expression rewrite(final Expression expression) {
    final Expression pseudoTimestamp = new StatementRewriteForMagicPseudoTimestamp()
        .rewrite(expression);
    final Expression inPredicatesRemoved = rewriteInPredicates(pseudoTimestamp);
    return LogicRewriter.rewriteDNF(inPredicatesRemoved);
  }

  public static Expression rewriteInPredicates(final Expression expression) {
    return new ExpressionTreeRewriter<>(new InPredicateRewriter()::process)
        .rewrite(expression, null);
  }

  private static final class InPredicateRewriter extends
      VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    @Override
    public Optional<Expression> visitExpression(
        final Expression node,
        final Context<Void> context) {
      return Optional.empty();
    }

    @Override
    public Optional<Expression> visitInPredicate(
        final InPredicate node,
        final Context<Void> context
    ) {
      Expression currentExpression = null;
      for (Expression inValueListExp : Lists.reverse(node.getValueList().getValues())) {
        final ComparisonExpression comparisonExpression = new ComparisonExpression(
            node.getLocation(), Type.EQUAL, node.getValue(),
            inValueListExp);
        if (currentExpression == null) {
          currentExpression = comparisonExpression;
          continue;
        }
        currentExpression = new LogicalBinaryExpression(
            node.getLocation(), LogicalBinaryExpression.Type.OR, comparisonExpression,
            currentExpression);
      }
      if (currentExpression != null) {
        return Optional.of(currentExpression);
      }
      throw new IllegalStateException("Shouldn't have an empty in predicate");
    }
  }
}
