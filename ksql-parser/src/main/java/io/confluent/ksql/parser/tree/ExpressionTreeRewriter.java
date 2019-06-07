/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.parser.tree;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.confluent.ksql.util.Pair;
import java.util.Iterator;
import java.util.Optional;

public final class ExpressionTreeRewriter<C> {

  private final ExpressionRewriter<C> rewriter;
  private final AstVisitor<Expression, Context<C>> visitor;

  public static <C, T extends Expression> T rewriteWith(
      final ExpressionRewriter<C> rewriter, final T node) {
    return new ExpressionTreeRewriter<>(rewriter).rewrite(node, null);
  }


  public ExpressionTreeRewriter(final ExpressionRewriter<C> rewriter) {
    this.rewriter = rewriter;
    this.visitor = new RewritingVisitor();
  }

  @SuppressWarnings("unchecked")
  public <T extends Expression> T rewrite(final T node, final C context) {
    return (T) visitor.process(node, new Context<>(context, false));
  }

  private class RewritingVisitor
      extends AstVisitor<Expression, Context<C>> {

    @Override
    protected Expression visitExpression(final Expression node, final Context<C> context) {
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
    protected Expression visitStruct(final Struct node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression result = rewriter.rewriteStruct(node, context.get(), ExpressionTreeRewriter
            .this);
        if (result != null) {
          return result;
        }
      }

      final ImmutableList.Builder<Expression> builder = ImmutableList.builder();
      for (final Pair<String, Type> structItem : node.getItems()) {
        builder.add(rewrite(structItem.getRight(), context.get()));
      }
      return node;
    }

    @Override
    protected Expression visitArithmeticUnary(
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
        return new ArithmeticUnaryExpression(node.getSign(), child);
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
        return new ArithmeticBinaryExpression(node.getType(), left, right);
      }

      return node;
    }

    @Override
    protected Expression visitSubscriptExpression(
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
        return new SubscriptExpression(base, index);
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
        return new ComparisonExpression(node.getType(), left, right);
      }

      return node;
    }

    @Override
    protected Expression visitBetweenPredicate(
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
        return new LogicalBinaryExpression(node.getType(), left, right);
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
        return new NotExpression(value);
      }

      return node;
    }

    @Override
    protected Expression visitIsNullPredicate(
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
        return new IsNullPredicate(value);
      }

      return node;
    }

    @Override
    protected Expression visitIsNotNullPredicate(
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
        return new IsNotNullPredicate(value);
      }

      return node;
    }

    @Override
    protected Expression visitNullIfExpression(
        final NullIfExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteNullIfExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression first = rewrite(node.getFirst(), context.get());
      final Expression second = rewrite(node.getSecond(), context.get());

      if (first != node.getFirst() || second != node.getSecond()) {
        return new NullIfExpression(first, second);
      }

      return node;
    }

    @Override
    protected Expression visitSearchedCaseExpression(final SearchedCaseExpression node,
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
        return new SearchedCaseExpression(builder.build(), defaultValue);
      }

      return node;
    }

    @Override
    protected Expression visitSimpleCaseExpression(
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
        return new SimpleCaseExpression(operand, builder.build(), defaultValue);
      }

      return node;
    }

    @Override
    protected Expression visitWhenClause(final WhenClause node, final Context<C> context) {
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
        return new WhenClause(operand, result);
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
      Expression escape = null;
      if (node.getEscape() != null) {
        escape = rewrite(node.getEscape(), context.get());
      }

      if (value != node.getValue() || pattern != node.getPattern() || escape != node.getEscape()) {
        return new LikePredicate(value, pattern, escape);
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
      final Expression list = rewrite(node.getValueList(), context.get());

      if (node.getValue() != value || node.getValueList() != list) {
        return new InPredicate(value, list);
      }

      return node;
    }

    @Override
    protected Expression visitInListExpression(
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
        return new InListExpression(builder.build());
      }

      return node;
    }

    @Override
    protected Expression visitExists(final ExistsPredicate node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteExists(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      // No default rewrite for ExistsPredicate since we do not want to traverse subqueries
      return node;
    }

    @Override
    public Expression visitSubqueryExpression(
        final SubqueryExpression node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteSubqueryExpression(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      // No default rewrite for SubqueryExpression since we do not want to traverse subqueries
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
        return new DereferenceExpression(base, node.getFieldName());
      }

      return node;
    }

    @Override
    protected Expression visitExtract(final Extract node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteExtract(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      final Expression expression = rewrite(node.getExpression(), context.get());

      if (node.getExpression() != expression) {
        return new Extract(expression, node.getField());
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
        return new Cast(expression, node.getType(), node.isSafe(), node.isTypeOnly());
      }

      return node;
    }

    @Override
    protected Expression visitFieldReference(final FieldReference node, final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteFieldReference(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    protected Expression visitSymbolReference(
        final SymbolReference node,
        final Context<C> context) {
      if (!context.isDefaultRewrite()) {
        final Expression
            result =
            rewriter.rewriteSymbolReference(node, context.get(), ExpressionTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }
  }

  public static class Context<C> {

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
