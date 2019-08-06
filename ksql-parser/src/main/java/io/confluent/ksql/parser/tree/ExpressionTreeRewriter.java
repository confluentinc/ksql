/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public final class ExpressionTreeRewriter<C> {

  private final ExpressionRewriter<C> rewriter;
  private final ExpressionVisitor<Expression, Context<C>> visitor;

  public static <C, T extends Expression> T rewriteWith(
      final ExpressionRewriter<C> rewriter, final T node) {
    return new ExpressionTreeRewriter<>(rewriter).rewrite(node, null);
  }


  private ExpressionTreeRewriter(final ExpressionRewriter<C> rewriter) {
    this.rewriter = rewriter;
    this.visitor = new RewritingVisitor();
  }

  @SuppressWarnings("unchecked")
  public <T extends Expression> T rewrite(final T node, final C context) {
    return (T) visitor.process(node, new Context<>(context, false));
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private class RewritingVisitor
      extends VisitParentExpressionVisitor<Expression, Context<C>> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

    @Override
    public Expression visitExpression(final Expression node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      throw new UnsupportedOperationException(
          "not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass()
              .getName());
    }

    @Override
    public Expression visitType(final Type node, final Context<C> context) {
      if (!(node.getSqlType() instanceof SqlStruct)) {
        return node;
      }

      if (!context.isDefaultRewrite()) {
        final Expression result =
            rewriter.rewriteStruct(node, context.get(), ExpressionTreeRewriter.this);

        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Expression visitArithmeticUnary(
        final ArithmeticUnaryExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteArithmeticUnary(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression child = rewrite(node.getValue(), context.get());
      if (child != node.getValue()) {
        return new ArithmeticUnaryExpression(node.getLocation(), node.getSign(), child);
      }

      return node;
    }

    @Override
    public Expression visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteArithmeticBinary(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression left = rewrite(node.getLeft(), context.get());
      final Expression right = rewrite(node.getRight(), context.get());

      if (left != node.getLeft() || right != node.getRight()) {
        return new ArithmeticBinaryExpression(node.getLocation(), node.getOperator(), left, right);
      }

      return node;
    }

    @Override
    public Expression visitSubscriptExpression(
        final SubscriptExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteSubscriptExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression base = rewrite(node.getBase(), context.get());
      final Expression index = rewrite(node.getIndex(), context.get());

      if (base != node.getBase() || index != node.getIndex()) {
        return new SubscriptExpression(node.getLocation(), base, index);
      }

      return node;
    }

    @Override
    public Expression visitComparisonExpression(
        final ComparisonExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteComparisonExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression left = rewrite(node.getLeft(), context.get());
      final Expression right = rewrite(node.getRight(), context.get());

      if (left != node.getLeft() || right != node.getRight()) {
        return new ComparisonExpression(node.getLocation(), node.getType(), left, right);
      }

      return node;
    }

    @Override
    public Expression visitBetweenPredicate(
        final BetweenPredicate node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteBetweenPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression value = rewrite(node.getValue(), context.get());
      final Expression min = rewrite(node.getMin(), context.get());
      final Expression max = rewrite(node.getMax(), context.get());

      if (value != node.getValue() || min != node.getMin() || max != node.getMax()) {
        return new BetweenPredicate(value, min, max);
      }

      return node;
    }

    @Override
    public Expression visitLogicalBinaryExpression(final LogicalBinaryExpression node,
                                                   final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter
                .rewriteLogicalBinaryExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression left = rewrite(node.getLeft(), context.get());
      final Expression right = rewrite(node.getRight(), context.get());

      if (left != node.getLeft() || right != node.getRight()) {
        return new LogicalBinaryExpression(node.getLocation(), node.getType(), left, right);
      }

      return node;
    }

    @Override
    public Expression visitNotExpression(final NotExpression node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteNotExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new NotExpression(node.getLocation(), value);
      }

      return node;
    }

    @Override
    public Expression visitIsNullPredicate(
        final IsNullPredicate node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteIsNullPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new IsNullPredicate(node.getLocation(), value);
      }

      return node;
    }

    @Override
    public Expression visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteIsNotNullPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new IsNotNullPredicate(node.getLocation(), value);
      }

      return node;
    }

    @Override
    public Expression visitSearchedCaseExpression(final SearchedCaseExpression node,
                                                     final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter
                .rewriteSearchedCaseExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
      for (final WhenClause expression : node.getWhenClauses()) {
        builder.add(rewrite(expression, context.get()));
      }

      final Optional<Expression> defaultValue = node.getDefaultValue()
          .map(value -> rewrite(value, context.get()));

      if (!sameElements(node.getDefaultValue(), defaultValue) || !sameElements(
          node.getWhenClauses(), builder.build())) {
        return new SearchedCaseExpression(node.getLocation(), builder.build(), defaultValue);
      }

      return node;
    }

    @Override
    public Expression visitSimpleCaseExpression(
        final SimpleCaseExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteSimpleCaseExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression operand = rewrite(node.getOperand(), context.get());

      final ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
      for (final WhenClause expression : node.getWhenClauses()) {
        builder.add(rewrite(expression, context.get()));
      }

      final Optional<Expression> defaultValue = node.getDefaultValue()
          .map(value -> rewrite(value, context.get()));

      if (operand != node.getOperand()
          || !sameElements(node.getDefaultValue(), defaultValue)
          || !sameElements(node.getWhenClauses(), builder.build())) {
        return new SimpleCaseExpression(node.getLocation(), operand, builder.build(), defaultValue);
      }

      return node;
    }

    @Override
    public Expression visitWhenClause(final WhenClause node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteWhenClause(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression operand = rewrite(node.getOperand(), context.get());
      final Expression result = rewrite(node.getResult(), context.get());

      if (operand != node.getOperand() || result != node.getResult()) {
        return new WhenClause(node.getLocation(), operand, result);
      }
      return node;
    }

    @Override
    public Expression visitFunctionCall(final FunctionCall node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteFunctionCall(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final List<Expression> args = node.getArguments().stream()
          .map(arg -> rewrite(arg, context.get()))
          .collect(Collectors.toList());

      if (!node.getArguments().equals(args)) {
        return new FunctionCall(node.getLocation(), node.getName(), args);
      }

      return node;
    }

    @Override
    public Expression visitLikePredicate(final LikePredicate node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteLikePredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression value = rewrite(node.getValue(), context.get());
      final Expression pattern = rewrite(node.getPattern(), context.get());

      if (value != node.getValue() || pattern != node.getPattern()) {
        return new LikePredicate(node.getLocation(), value, pattern);
      }

      return node;
    }

    @Override
    public Expression visitInPredicate(final InPredicate node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteInPredicate(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression value = rewrite(node.getValue(), context.get());
      final InListExpression list = rewrite(node.getValueList(), context.get());

      if (node.getValue() != value || node.getValueList() != list) {
        return new InPredicate(node.getLocation(), value, list);
      }

      return node;
    }

    @Override
    public Expression visitInListExpression(
        final InListExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteInListExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final ImmutableList.Builder<Expression> builder = ImmutableList.builder();
      for (final Expression expression : node.getValues()) {
        builder.add(rewrite(expression, context.get()));
      }

      if (!sameElements(node.getValues(), builder.build())) {
        return new InListExpression(node.getLocation(), builder.build());
      }

      return node;
    }

    @Override
    public Expression visitLiteral(final Literal node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteLiteral(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Expression visitQualifiedNameReference(
        final QualifiedNameReference node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter
                .rewriteQualifiedNameReference(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Expression visitDereferenceExpression(
        final DereferenceExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteDereferenceExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression base = rewrite(node.getBase(), context.get());
      if (base != node.getBase()) {
        return new DereferenceExpression(node.getLocation(), base, node.getFieldName());
      }

      return node;
    }

    @Override
    public Expression visitCast(final Cast node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression result =
            rewriter.rewriteCast(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression expression = rewrite(node.getExpression(), context.get());

      if (node.getExpression() != expression) {
        return new Cast(node.getLocation(), expression, node.getType());
      }

      return node;
    }
  }

  public static final class Context<C> {

    private final boolean defaultRewrite;
    private final C context;

    private Context(final C context, final boolean defaultRewrite) {
      this.context = context;
      this.defaultRewrite = defaultRewrite;
    }

    public C get() {
      return context;
    }

    public boolean isDefaultRewrite() {
      return defaultRewrite;
    }
  }

  private static <T> boolean sameElements(final Optional<T> a, final Optional<T> b) {
    if (!a.isPresent() && !b.isPresent()) {
      return true;
    } else if (a.isPresent() != b.isPresent()) {
      return false;
    }

    return a.get() == b.get();
  }

  @SuppressWarnings("ObjectEquality")
  private static <T> boolean sameElements(
      final Iterable<? extends T> a,
      final Iterable<? extends T> b) {
    if (Iterables.size(a) != Iterables.size(b)) {
      return false;
    }

    final Iterator<? extends T> first = a.iterator();
    final Iterator<? extends T> second = b.iterator();

    while (first.hasNext() && second.hasNext()) {
      if (first.next() != second.next()) {
        return false;
      }
    }

    return true;
  }
}
