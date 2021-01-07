package io.confluent.ksql.planner.plan;

import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import java.util.Optional;

public class PullQueryRewriter {

  public static Expression rewrite(final Expression expression) {
    Expression inPredicatesRemoved = rewriteInPredicates(expression);
    return LogicRewriter.rewriteDNF(inPredicatesRemoved);
  }

  public static Expression rewriteInPredicates(final Expression expression) {
    return new ExpressionTreeRewriter<>(new InPredicateRewriter()::process)
        .rewrite(expression, null);
  }

  private final static class InPredicateRewriter extends
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
      for (Expression inValueListExp : node.getValueList().getValues()) {
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
