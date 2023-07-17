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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.ExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IntervalUnit;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaVariable;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * ExpressionTreeRewriter creates a copy of an expression with sub-expressions optionally
 * rewritten by a plugin. The plugin implements
 * {@code BiFunction<Expression, Context<C>, Optional<Expression>>}. The plugin
 * will be called for each expression within the provided expression (including for the
 * provided expression). If the plugin returns a non-empty value, the returned value will
 * be substituted for the expression in the final result.
 *
 * @param <C> A context type to be passed through to the plugin.
 */
public final class ExpressionTreeRewriter<C> {

  public static final class Context<C> {
    private final C context;
    private final ExpressionVisitor<Expression, C> rewriter;

    private Context(final C context, final ExpressionVisitor<Expression, C> rewriter) {
      this.context = context;
      this.rewriter = rewriter;
    }

    public C getContext() {
      return context;
    }

    public Expression process(final Expression expression) {
      return rewriter.process(expression, context);
    }
  }

  private final RewritingVisitor<C> rewriter;

  public static <C, T extends Expression> T rewriteWith(
      final BiFunction<Expression, Context<C>, Optional<Expression>> plugin, final T expression) {
    return rewriteWith(plugin, expression, null);
  }

  public static <C, T extends Expression> T rewriteWith(
      final BiFunction<Expression, Context<C>, Optional<Expression>> plugin,
      final T expression,
      final C context) {
    return new ExpressionTreeRewriter<>(plugin).rewrite(expression, context);
  }

  @SuppressWarnings("unchecked")
  public <T extends Expression> T rewrite(final T expression, final C context) {
    return (T) rewriter.process(expression, context);
  }

  public ExpressionTreeRewriter(
      final BiFunction<Expression, Context<C>, Optional<Expression>> plugin) {
    this.rewriter = new RewritingVisitor<>(plugin);
  }

  // Exposed at package-level for testing
  ExpressionTreeRewriter(
      final BiFunction<Expression, Context<C>, Optional<Expression>> plugin,
      final BiFunction<Expression, C, Expression> rewriter) {
    this.rewriter = new RewritingVisitor<>(plugin, rewriter);
  }

  // CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
  private static final class RewritingVisitor<C> implements ExpressionVisitor<Expression, C> {
    // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling
    private final BiFunction<Expression, Context<C>, Optional<Expression>> plugin;
    private final BiFunction<Expression, C, Expression> rewriter;

    private RewritingVisitor(
        final BiFunction<Expression, Context<C>, Optional<Expression>> plugin) {
      this.plugin = Objects.requireNonNull(plugin, "plugin");
      this.rewriter = this::process;
    }

    private RewritingVisitor(
        final BiFunction<Expression, Context<C>, Optional<Expression>> plugin,
        final BiFunction<Expression, C, Expression> rewriter) {
      this.plugin = Objects.requireNonNull(plugin, "plugin");
      this.rewriter = Objects.requireNonNull(rewriter, "rewriter");
    }

    @Override
    public Expression visitType(final Type node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitArithmeticUnary(
        final ArithmeticUnaryExpression node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }
      final Expression child = rewriter.apply(node.getValue(), context);
      return new ArithmeticUnaryExpression(node.getLocation(), node.getSign(), child);
    }

    @Override
    public Expression visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression left = rewriter.apply(node.getLeft(), context);
      final Expression right = rewriter.apply(node.getRight(), context);

      return new ArithmeticBinaryExpression(node.getLocation(), node.getOperator(), left, right);
    }

    @Override
    public Expression visitSubscriptExpression(
        final SubscriptExpression node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression base = rewriter.apply(node.getBase(), context);
      final Expression index = rewriter.apply(node.getIndex(), context);

      return new SubscriptExpression(node.getLocation(), base, index);
    }

    @Override
    public Expression visitCreateArrayExpression(final CreateArrayExpression exp, final C context) {
      final Builder<Expression> values = ImmutableList.builder();
      for (Expression value : exp.getValues()) {
        values.add(rewriter.apply(value, context));
      }
      return new CreateArrayExpression(exp.getLocation(), values.build());
    }

    @Override
    public Expression visitCreateMapExpression(final CreateMapExpression exp, final C context) {
      final ImmutableMap.Builder<Expression, Expression> map = ImmutableMap.builder();
      for (Entry<Expression, Expression> entry : exp.getMap().entrySet()) {
        map.put(
            rewriter.apply(entry.getKey(), context),
            rewriter.apply(entry.getValue(), context)
        );
      }
      return new CreateMapExpression(exp.getLocation(), map.build());
    }

    @Override
    public Expression visitStructExpression(final CreateStructExpression node, final C context) {
      final Builder<Field> fields = ImmutableList.builder();
      for (final Field field : node.getFields()) {
        fields.add(new Field(field.getName(), rewriter.apply(field.getValue(), context)));
      }
      return new CreateStructExpression(node.getLocation(), fields.build());
    }

    @Override
    public Expression visitComparisonExpression(
        final ComparisonExpression node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression left = rewriter.apply(node.getLeft(), context);
      final Expression right = rewriter.apply(node.getRight(), context);

      return new ComparisonExpression(node.getLocation(), node.getType(), left, right);
    }

    @Override
    public Expression visitBetweenPredicate(
        final BetweenPredicate node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression value = rewriter.apply(node.getValue(), context);
      final Expression min = rewriter.apply(node.getMin(), context);
      final Expression max = rewriter.apply(node.getMax(), context);

      return new BetweenPredicate(value, min, max);
    }

    @Override
    public Expression visitLogicalBinaryExpression(final LogicalBinaryExpression node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression left = rewriter.apply(node.getLeft(), context);
      final Expression right = rewriter.apply(node.getRight(), context);
      return new LogicalBinaryExpression(node.getLocation(), node.getType(), left, right);
    }

    @Override
    public Expression visitNotExpression(final NotExpression node, final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression value = rewriter.apply(node.getValue(), context);
      return new NotExpression(node.getLocation(), value);
    }

    @Override
    public Expression visitIsNullPredicate(
        final IsNullPredicate node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression value = rewriter.apply(node.getValue(), context);

      return new IsNullPredicate(node.getLocation(), value);
    }

    @Override
    public Expression visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression value = rewriter.apply(node.getValue(), context);

      return new IsNotNullPredicate(node.getLocation(), value);
    }

    @Override
    public Expression visitSearchedCaseExpression(final SearchedCaseExpression node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
      for (final WhenClause expression : node.getWhenClauses()) {
        builder.add((WhenClause) rewriter.apply(expression, context));
      }
      final Optional<Expression> defaultValue = node.getDefaultValue()
          .map(value -> rewriter.apply(value, context));
      return new SearchedCaseExpression(node.getLocation(), builder.build(), defaultValue);
    }

    @Override
    public Expression visitSimpleCaseExpression(
        final SimpleCaseExpression node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression operand = rewriter.apply(node.getOperand(), context);

      final ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
      for (final WhenClause expression : node.getWhenClauses()) {
        builder.add((WhenClause) rewriter.apply(expression, context));
      }

      final Optional<Expression> defaultValue = node.getDefaultValue()
          .map(value -> rewriter.apply(value, context));

      return new SimpleCaseExpression(node.getLocation(), operand, builder.build(), defaultValue);
    }

    @Override
    public Expression visitWhenClause(final WhenClause node, final C context) {
      final Optional<Expression> rewritten
          = plugin.apply(node, new Context<>(context, this));
      if (rewritten.isPresent()) {
        return rewritten.get();
      }

      final Expression operand = rewriter.apply(node.getOperand(), context);
      final Expression result = rewriter.apply(node.getResult(), context);

      return new WhenClause(node.getLocation(), operand, result);
    }

    @Override
    public Expression visitFunctionCall(final FunctionCall node, final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final List<Expression> args = node.getArguments().stream()
          .map(arg -> rewriter.apply(arg, context))
          .collect(Collectors.toList());

      return new FunctionCall(node.getLocation(), node.getName(), args);
    }

    @Override
    public Expression visitLikePredicate(final LikePredicate node, final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression value = rewriter.apply(node.getValue(), context);
      final Expression pattern = rewriter.apply(node.getPattern(), context);

      return new LikePredicate(node.getLocation(), value, pattern, node.getEscape());
    }

    @Override
    public Expression visitInPredicate(final InPredicate node, final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression value = rewriter.apply(node.getValue(), context);
      final InListExpression list = (InListExpression) rewriter.apply(node.getValueList(), context);

      return new InPredicate(node.getLocation(), value, list);
    }

    @Override
    public Expression visitInListExpression(final InListExpression node, final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final ImmutableList.Builder<Expression> builder = ImmutableList.builder();
      for (final Expression expression : node.getValues()) {
        builder.add(rewriter.apply(expression, context));
      }

      return new InListExpression(node.getLocation(), builder.build());
    }

    @Override
    public Expression visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitDereferenceExpression(
        final DereferenceExpression node,
        final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression base = rewriter.apply(node.getBase(), context);
      return new DereferenceExpression(node.getLocation(), base, node.getFieldName());
    }

    @Override
    public Expression visitCast(final Cast node, final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression expression = rewriter.apply(node.getExpression(), context);
      final Type type = (Type) rewriter.apply(node.getType(), context);
      return new Cast(node.getLocation(), expression, type);
    }

    @Override
    public Expression visitLambdaExpression(final LambdaFunctionCall node, final C context) {
      final Optional<Expression> result
          = plugin.apply(node, new Context<>(context, this));
      if (result.isPresent()) {
        return result.get();
      }

      final Expression expression = rewriter.apply(node.getBody(), context);
      return new LambdaFunctionCall(node.getLocation(), node.getArguments(), expression);
    }

    @Override
    public Expression visitBooleanLiteral(final BooleanLiteral node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitDoubleLiteral(final DoubleLiteral node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitIntegerLiteral(final IntegerLiteral node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitLongLiteral(final LongLiteral node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitLambdaVariable(final LambdaVariable node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitIntervalUnit(final IntervalUnit node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitNullLiteral(final NullLiteral node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitStringLiteral(final StringLiteral node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitDecimalLiteral(final DecimalLiteral node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitTimeLiteral(final TimeLiteral node, final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }

    @Override
    public Expression visitTimestampLiteral(
        final TimestampLiteral node,
        final C context) {
      return plugin.apply(node, new Context<>(context, this)).orElse(node);
    }
  }
}
