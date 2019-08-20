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

package io.confluent.ksql.execution.expression.formatter;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.DecimalLiteral;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.DoubleLiteral;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.ExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.InListExpression;
import io.confluent.ksql.execution.expression.tree.InPredicate;
import io.confluent.ksql.execution.expression.tree.IntegerLiteral;
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.QualifiedName;
import io.confluent.ksql.execution.expression.tree.QualifiedNameReference;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlConstants;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

public final class ExpressionFormatter {
  private ExpressionFormatter() {
  }

  public static String formatExpression(final Expression expression) {
    return formatExpression(expression, true, s -> false);
  }

  public static String formatExpression(
      final Expression expression,
      final boolean unmangleNames,
      final Predicate<String> isReserved) {
    return new Formatter().process(expression, new Context(unmangleNames, isReserved));
  }

  private static final class Context {
    final boolean unmangleNames;
    final Predicate<String> isReserved;

    private Context(final boolean unmangleNames, final Predicate<String> isReserved) {
      this.unmangleNames = unmangleNames;
      this.isReserved = isReserved;
    }
  }

  private static class Formatter implements ExpressionVisitor<String, Context> {
    @Override
    public String visitType(final Type node, final Context context) {
      return node.getSqlType().toString(FormatOptions.of(context.isReserved));
    }

    @Override
    public String visitBooleanLiteral(final BooleanLiteral node, final Context context) {
      return String.valueOf(node.getValue());
    }

    @Override
    public String visitStringLiteral(final StringLiteral node, final Context context) {
      return formatStringLiteral(node.getValue());
    }

    @Override
    public String visitSubscriptExpression(
        final SubscriptExpression node,
        final Context context) {
      return process(node.getBase(), context)
          + "[" + process(node.getIndex(), context) + "]";
    }

    @Override
    public String visitLongLiteral(final LongLiteral node, final Context context) {
      return Long.toString(node.getValue());
    }

    @Override
    public String visitIntegerLiteral(final IntegerLiteral node, final Context context) {
      return Integer.toString(node.getValue());
    }

    @Override
    public String visitDoubleLiteral(final DoubleLiteral node, final Context context) {
      return Double.toString(node.getValue());
    }

    @Override
    public String visitDecimalLiteral(final DecimalLiteral node, final Context context) {
      return "DECIMAL '" + node.getValue() + "'";
    }

    @Override
    public String visitTimeLiteral(final TimeLiteral node, final Context context) {
      return "TIME '" + node.getValue() + "'";
    }

    @Override
    public String visitTimestampLiteral(
        final TimestampLiteral node,
        final Context context) {
      return "TIMESTAMP '" + node.getValue() + "'";
    }

    @Override
    public String visitNullLiteral(final NullLiteral node, final Context context) {
      return "null";
    }

    @Override
    public String visitQualifiedNameReference(final QualifiedNameReference node,
        final Context context) {
      return formatQualifiedName(node.getName(), context);
    }

    @Override
    public String visitDereferenceExpression(
        final DereferenceExpression node,
        final Context context) {
      final String baseString = process(node.getBase(), context);
      if (node.getBase() instanceof QualifiedNameReference) {
        return baseString + KsqlConstants.DOT + formatIdentifier(node.getFieldName(), context);
      }
      return baseString + KsqlConstants.STRUCT_FIELD_REF
          + formatIdentifier(node.getFieldName(), context);
    }

    private static String formatQualifiedName(final QualifiedName name, final Context context) {
      final List<String> parts = new ArrayList<>();
      for (final String part : name.getParts()) {
        parts.add(formatIdentifier(part, context));
      }
      return Joiner.on(KsqlConstants.DOT).join(parts);
    }

    @Override
    public String visitFunctionCall(final FunctionCall node, final Context context) {
      final StringBuilder builder = new StringBuilder();

      String arguments = joinExpressions(node.getArguments(), context);
      if (node.getArguments().isEmpty() && "COUNT".equals(node.getName().getSuffix())) {
        arguments = "*";
      }

      builder.append(formatQualifiedName(node.getName(), context))
          .append('(').append(arguments).append(')');

      return builder.toString();
    }

    @Override
    public String visitLogicalBinaryExpression(final LogicalBinaryExpression node,
        final Context context) {
      return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight(),
          context);
    }

    @Override
    public String visitNotExpression(final NotExpression node, final Context context) {
      return "(NOT " + process(node.getValue(), context) + ")";
    }

    @Override
    public String visitComparisonExpression(
        final ComparisonExpression node,
        final Context context) {
      return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(),
          context);
    }

    @Override
    public String visitIsNullPredicate(final IsNullPredicate node, final Context context) {
      return "(" + process(node.getValue(), context) + " IS NULL)";
    }

    @Override
    public String visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final Context context) {
      return "(" + process(node.getValue(), context) + " IS NOT NULL)";
    }

    @Override
    public String visitArithmeticUnary(
        final ArithmeticUnaryExpression node,
        final Context context) {
      final String value = process(node.getValue(), context);

      switch (node.getSign()) {
        case MINUS:
          // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
          final String separator = value.startsWith("-") ? " " : "";
          return "-" + separator + value;
        case PLUS:
          return "+" + value;
        default:
          throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
      }
    }

    @Override
    public String visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final Context context) {
      return formatBinaryExpression(node.getOperator().getSymbol(), node.getLeft(), node.getRight(),
          context);
    }

    @Override
    public String visitLikePredicate(final LikePredicate node, final Context context) {
      return "("
          + process(node.getValue(), context)
          + " LIKE "
          + process(node.getPattern(), context)
          + ')';
    }

    @Override
    public String visitCast(final Cast node, final Context context) {
      return "CAST"
          + "(" + process(node.getExpression(), context) + " AS " + node.getType() + ")";
    }

    @Override
    public String visitSearchedCaseExpression(final SearchedCaseExpression node,
        final Context context) {
      final ImmutableList.Builder<String> parts = ImmutableList.builder();
      parts.add("CASE");
      for (final WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, context));
      }

      node.getDefaultValue()
          .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitSimpleCaseExpression(
        final SimpleCaseExpression node,
        final Context context) {
      final ImmutableList.Builder<String> parts = ImmutableList.builder();

      parts.add("CASE")
          .add(process(node.getOperand(), context));

      for (final WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, context));
      }

      node.getDefaultValue()
          .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitWhenClause(final WhenClause node, final Context context) {
      return "WHEN " + process(node.getOperand(), context) + " THEN " + process(
          node.getResult(), context);
    }

    @Override
    public String visitBetweenPredicate(
        final BetweenPredicate node,
        final Context context) {
      return "(" + process(node.getValue(), context) + " BETWEEN "
          + process(node.getMin(), context) + " AND " + process(node.getMax(),
          context)
          + ")";
    }

    @Override
    public String visitInPredicate(final InPredicate node, final Context context) {
      return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(),
          context) + ")";
    }

    @Override
    public String visitInListExpression(
        final InListExpression node,
        final Context context) {
      return "(" + joinExpressions(node.getValues(), context) + ")";
    }

    private String formatBinaryExpression(
        final String operator, final Expression left, final Expression right,
        final Context context) {
      return '(' + process(left, context) + ' ' + operator + ' ' + process(right,
          context)
          + ')';
    }

    private String joinExpressions(
        final List<Expression> expressions,
        final Context context) {
      return Joiner.on(", ").join(expressions.stream()
          .map((e) -> process(e, context))
          .iterator());
    }

    private static String formatIdentifier(final String s, final Context context) {
      return context.isReserved.test(s) ? "`" + s + "`" : s;
    }

    private static String formatStringLiteral(final String s) {
      return "'" + s.replace("'", "''") + "'";
    }
  }
}
