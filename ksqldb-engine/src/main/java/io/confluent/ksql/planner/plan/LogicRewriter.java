package io.confluent.ksql.planner.plan;

import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter;
import io.confluent.ksql.engine.rewrite.ExpressionTreeRewriter.Context;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression.Type;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class LogicRewriter {


  public LogicRewriter() {
  }

  public static Expression rewriteNegations(final Expression expression) {
    return new ExpressionTreeRewriter<>(new NotPropagator()::process)
        .rewrite(expression, new NotPropagatorContext());
  }

  public static Expression rewriteCNF(final Expression expression) {
    Expression notPropagated = new ExpressionTreeRewriter<>(new NotPropagator()::process)
          .rewrite(expression, new NotPropagatorContext());
    return new ExpressionTreeRewriter<>(new DistributiveLawApplierDOC()::process)
        .rewrite(notPropagated, null);
  }

  public static Expression rewriteDNF(final Expression expression) {
    Expression notPropagated = new ExpressionTreeRewriter<>(new NotPropagator()::process)
        .rewrite(expression, new NotPropagatorContext());
    return new ExpressionTreeRewriter<>(new DistributiveLawApplierCOD()::process)
        .rewrite(notPropagated, null);
  }

  public static List<Expression> extractDisjuncts(final Expression expression) {
    final Expression dnf = rewriteDNF(expression);
    final DisjunctExtractor disjunctExtractor = new DisjunctExtractor();
    disjunctExtractor.process(dnf, null);
    return disjunctExtractor.getConjunctions();
  }

  private final static class NotPropagator extends
      VisitParentExpressionVisitor<Optional<Expression>, Context<NotPropagatorContext>> {

    public NotPropagator() {
    }

    @Override
    public Optional<Expression> visitExpression(
        final Expression node,
        final Context<NotPropagatorContext> context) {
      return Optional.empty();
    }

    @Override
    public Optional<Expression> visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Context<NotPropagatorContext> context
    ) {
      return handlePrimitiveTerm(node, context);
    }

    @Override
    public Optional<Expression> visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Context<NotPropagatorContext> context) {
      return handlePrimitiveTerm(node, context);
    }

    @Override
    public Optional<Expression> visitBooleanLiteral(
        final BooleanLiteral node,
        final Context<NotPropagatorContext> context) {
      return handlePrimitiveTerm(node, context);
    }

    private Optional<Expression> handlePrimitiveTerm(
        final Expression node,
        final Context<NotPropagatorContext> context) {
      if (!context.getContext().isNegated()) {
        return Optional.empty();
      }
      return Optional.of(new NotExpression(node.getLocation(), node));
    }

    @Override
    public Optional<Expression> visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Context<NotPropagatorContext> context
    ) {
      boolean isNegated = context.getContext().isNegated();
      Expression left = process(node.getLeft(), context).orElse(node.getLeft());
      context.getContext().restore(isNegated);
      Expression right = process(node.getRight(), context).orElse(node.getRight());
      context.getContext().restore(isNegated);

      LogicalBinaryExpression.Type type = node.getType();
      if (isNegated) {
        type = node.getType() == LogicalBinaryExpression.Type.AND
            ? LogicalBinaryExpression.Type.OR : LogicalBinaryExpression.Type.AND;
      }
      return Optional.of(new LogicalBinaryExpression(node.getLocation(), type, left, right));
    }

    @Override
    public Optional<Expression> visitComparisonExpression(
        final ComparisonExpression node,
        final Context<NotPropagatorContext> context
    ) {
      if (!context.getContext().isNegated()) {
        return Optional.empty();
      }
      return Optional.of(
          new ComparisonExpression(
              node.getLocation(), negateComparisonType(node.getType()), node.getLeft(),
              node.getRight()));
    }

    private Type negateComparisonType(Type type) {
      switch (type) {
        case EQUAL:
          return Type.NOT_EQUAL;
        case NOT_EQUAL:
          return Type.EQUAL;
        case LESS_THAN:
          return Type.GREATER_THAN_OR_EQUAL;
        case GREATER_THAN_OR_EQUAL:
          return Type.LESS_THAN;
        case GREATER_THAN:
          return Type.LESS_THAN_OR_EQUAL;
        case LESS_THAN_OR_EQUAL:
          return Type.GREATER_THAN;
        case IS_DISTINCT_FROM:
          return Type.IS_NOT_DISTINCT_FROM;
        case IS_NOT_DISTINCT_FROM:
          return Type.IS_DISTINCT_FROM;
        default:
          throw new KsqlException("Unknown type " + type);
      }
    }

    @Override
    public Optional<Expression> visitNotExpression(
        final NotExpression node, final Context<NotPropagatorContext> context) {
      context.getContext().negate();
      Expression value = process(node.getValue(), context).orElse(node.getValue());
      return Optional.of(value);
    }
  }

  public static class NotPropagatorContext {
    boolean isNegated = false;

    public void negate() {
      isNegated = !isNegated;
    }

    public boolean isNegated() {
      return isNegated;
    }

    public void restore(final boolean isNegated) {
      this.isNegated = isNegated;
    }
  }

  private final static class DistributiveLawApplierDOC extends
      VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    @Override
    public Optional<Expression> visitExpression(
        final Expression node,
        final Context<Void> context) {
      return Optional.empty();
    }

    @Override
    public Optional<Expression> visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Context<Void> context
    ) {
      final boolean isLeftLogicalExp = node.getLeft() instanceof LogicalBinaryExpression;
      final boolean isRightLogicalExp = node.getRight() instanceof LogicalBinaryExpression;
      if (!isLeftLogicalExp && !isRightLogicalExp) {
        return Optional.empty();
      }

      Expression left = process(node.getLeft(), context).orElse(node.getLeft());
      Expression right = process(node.getRight(), context).orElse(node.getRight());

      if (node.getType() == LogicalBinaryExpression.Type.OR) {
        if (left instanceof LogicalBinaryExpression) {
          LogicalBinaryExpression leftLogical = (LogicalBinaryExpression) left;
          if (leftLogical.getType() == LogicalBinaryExpression.Type.AND) {
            Expression leftOr = new LogicalBinaryExpression(node.getLocation(),
                LogicalBinaryExpression.Type.OR, leftLogical.getLeft(), right);
            leftOr = process(leftOr, context).orElse(leftOr);
            Expression rightOr = new LogicalBinaryExpression(node.getLocation(),
                LogicalBinaryExpression.Type.OR, leftLogical.getRight(), right);
            rightOr = process(rightOr, context).orElse(rightOr);
            return Optional.of(
                new LogicalBinaryExpression(node.getLocation(), LogicalBinaryExpression.Type.AND,
                    leftOr, rightOr));
          }
        }

        if (right instanceof LogicalBinaryExpression) {
          LogicalBinaryExpression rightLogical = (LogicalBinaryExpression) right;
          if (rightLogical.getType() == LogicalBinaryExpression.Type.AND) {
            Expression leftOr = new LogicalBinaryExpression(node.getLocation(),
                LogicalBinaryExpression.Type.OR, left, rightLogical.getLeft());
            leftOr = process(leftOr, context).orElse(leftOr);
            Expression rightOr = new LogicalBinaryExpression(node.getLocation(),
                LogicalBinaryExpression.Type.OR, left, rightLogical.getRight());
            rightOr = process(rightOr, context).orElse(rightOr);
            return Optional.of(
                new LogicalBinaryExpression(node.getLocation(), LogicalBinaryExpression.Type.AND,
                    leftOr, rightOr));
          }
        }
      }
      return Optional.of(
          new LogicalBinaryExpression(node.getLocation(), node.getType(), left, right));
    }
  }

  private final static class DistributiveLawApplierCOD extends
      VisitParentExpressionVisitor<Optional<Expression>, Context<Void>> {

    @Override
    public Optional<Expression> visitExpression(
        final Expression node,
        final Context<Void> context) {
      return Optional.empty();
    }

    @Override
    public Optional<Expression> visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Context<Void> context
    ) {
      final boolean isLeftLogicalExp = node.getLeft() instanceof LogicalBinaryExpression;
      final boolean isRightLogicalExp = node.getRight() instanceof LogicalBinaryExpression;
      if (!isLeftLogicalExp && !isRightLogicalExp) {
        return Optional.empty();
      }

      Expression left = process(node.getLeft(), context).orElse(node.getLeft());
      Expression right = process(node.getRight(), context).orElse(node.getRight());

      if (node.getType() == LogicalBinaryExpression.Type.AND) {
        if (left instanceof LogicalBinaryExpression) {
          LogicalBinaryExpression leftLogical = (LogicalBinaryExpression) left;
          if (leftLogical.getType() == LogicalBinaryExpression.Type.OR) {
            Expression leftOr = new LogicalBinaryExpression(node.getLocation(),
                LogicalBinaryExpression.Type.AND, leftLogical.getLeft(), right);
            leftOr = process(leftOr, context).orElse(leftOr);
            Expression rightOr = new LogicalBinaryExpression(node.getLocation(),
                LogicalBinaryExpression.Type.AND, leftLogical.getRight(), right);
            rightOr = process(rightOr, context).orElse(rightOr);
            return Optional.of(
                new LogicalBinaryExpression(node.getLocation(), LogicalBinaryExpression.Type.OR,
                    leftOr, rightOr));
          }
        }

        if (right instanceof LogicalBinaryExpression) {
          LogicalBinaryExpression rightLogical = (LogicalBinaryExpression) right;
          if (rightLogical.getType() == LogicalBinaryExpression.Type.OR) {
            Expression leftOr = new LogicalBinaryExpression(node.getLocation(),
                LogicalBinaryExpression.Type.AND, left, rightLogical.getLeft());
            leftOr = process(leftOr, context).orElse(leftOr);
            Expression rightOr = new LogicalBinaryExpression(node.getLocation(),
                LogicalBinaryExpression.Type.AND, left, rightLogical.getRight());
            rightOr = process(rightOr, context).orElse(rightOr);
            return Optional.of(
                new LogicalBinaryExpression(node.getLocation(), LogicalBinaryExpression.Type.OR,
                    leftOr, rightOr));
          }
        }
      }
      return Optional.of(
          new LogicalBinaryExpression(node.getLocation(), node.getType(), left, right));
    }
  }

  private static final class DisjunctExtractor extends VisitParentExpressionVisitor<Void, Void> {
    private List<Expression> ands = new ArrayList<>();

    @Override
    public Void visitExpression(
        final Expression node,
        final Void context) {
      ands.add(node);
      return null;
    }

    @Override
    public Void visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Void context
    ) {
      if (node.getType() == LogicalBinaryExpression.Type.AND) {
        ands.add(node);
      } else {
        process(node.getLeft(), context);
        process(node.getRight(), context);
      }
      return null;
    }

    public List<Expression> getConjunctions() {
      return ands;
    }
  }

  private static final class ConjunctExtractor extends VisitParentExpressionVisitor<Void, Void> {
    private List<Expression> ors = new ArrayList<>();

    @Override
    public Void visitExpression(
        final Expression node,
        final Void context) {
      ors.add(node);
      return null;
    }

    @Override
    public Void visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Void context
    ) {
      if (node.getType() == LogicalBinaryExpression.Type.OR) {
        ors.add(node);
      } else {
        process(node.getLeft(), context);
        process(node.getRight(), context);
      }
      return null;
    }

    public List<Expression> getConjuncts() {
      return ors;
    }
  }
}
