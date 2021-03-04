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
import com.ibm.icu.number.Notation;
import com.ibm.icu.number.NumberFormatter;
import com.ibm.icu.number.Precision;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
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
import io.confluent.ksql.name.Name;
import io.confluent.ksql.schema.ksql.SqlTimestamps;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

public final class ExpressionFormatter {

  private ExpressionFormatter() {
  }

  public static String formatExpression(final Expression expression) {
    return formatExpression(expression, FormatOptions.of(s -> false));
  }

  public static String formatExpression(
      final Expression expression, final FormatOptions formatOptions
  ) {
    return new Formatter().process(expression, new Context(formatOptions));
  }

  private static final class Context {

    final FormatOptions formatOptions;

    private Context(final FormatOptions formatOptions) {
      this.formatOptions = formatOptions;
    }
  }

  private static class Formatter implements ExpressionVisitor<String, Context> {

    @Override
    public String visitType(final Type node, final Context context) {
      return node.getSqlType().toString(context.formatOptions);
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
    public String visitLambdaVariable(final LambdaVariable node, final Context context) {
      return String.valueOf(node.getValue());
    }

    @Override
    public String visitIntervalUnit(final IntervalUnit exp, final Context context) {
      return exp.getUnit().toString();
    }

    @Override
    public String visitSubscriptExpression(final SubscriptExpression node, final Context context) {
      return process(node.getBase(), context)
          + "[" + process(node.getIndex(), context) + "]";
    }

    @Override
    public String visitCreateArrayExpression(
        final CreateArrayExpression exp,
        final Context context
    ) {
      return exp
          .getValues()
          .stream()
          .map(val -> process(val, context))
          .collect(Collectors.joining(", ", "ARRAY[", "]"));
    }

    @Override
    public String visitCreateMapExpression(final CreateMapExpression exp, final Context context) {
      return exp
          .getMap()
          .entrySet()
          .stream()
          .map(entry ->
              process(entry.getKey(), context)
                  + ":="
                  + process(entry.getValue(), context))
          .collect(Collectors.joining(", ", "MAP(", ")"));
    }

    @Override
    public String visitStructExpression(final CreateStructExpression exp, final Context context) {
      return exp
          .getFields()
          .stream()
          .map(struct ->
              context.formatOptions.escape(struct.getName())
                  + ":="
                  + process(struct.getValue(), context))
          .collect(Collectors.joining(", ", "STRUCT(", ")"));
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
      // NOTE: the Precision.unlimited() uses a complex algorithm to determine
      // the correct way to format the double value in scientific notation
      // without losing precision - do not change this to use Double#toString
      // because this may format the literal as a decimal (not scientific notation)
      return NumberFormatter.with()
          .notation(Notation.scientific())
          .precision(Precision.unlimited())
          .locale(Locale.ROOT)
          .format(node.getValue())
          .toString();
    }

    @Override
    public String visitDecimalLiteral(final DecimalLiteral node, final Context context) {
      return node.getValue().toString();
    }

    @Override
    public String visitTimeLiteral(final TimeLiteral node, final Context context) {
      return "TIME '" + node.getValue() + "'";
    }

    @Override
    public String visitTimestampLiteral(final TimestampLiteral node, final Context context) {
      return SqlTimestamps.formatTimestamp(node.getValue());
    }

    @Override
    public String visitNullLiteral(final NullLiteral node, final Context context) {
      return "null";
    }

    @Override
    public String visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Context context
    ) {
      return node.getColumnName().toString(context.formatOptions);
    }

    @Override
    public String visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Context context) {
      return formatName(node.getQualifier(), context)
          + KsqlConstants.DOT + node.getColumnName().toString(context.formatOptions);
    }

    @Override
    public String visitDereferenceExpression(
        final DereferenceExpression node,
        final Context context
    ) {
      final String baseString = process(node.getBase(), context);
      return baseString + KsqlConstants.STRUCT_FIELD_REF
          + context.formatOptions.escape(node.getFieldName());
    }

    private static String formatName(final Name<?> name, final Context context) {
      return name.toString(context.formatOptions);
    }

    @Override
    public String visitFunctionCall(final FunctionCall node, final Context context) {
      final StringBuilder builder = new StringBuilder();

      String arguments = joinExpressions(node.getArguments(), context);
      if (node.getArguments().isEmpty() && "COUNT".equals(node.getName().text())) {
        arguments = "*";
      }

      builder.append(formatName(node.getName(), context))
          .append('(').append(arguments).append(')');

      return builder.toString();
    }

    @Override
    public String visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Context context
    ) {
      return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight(),
          context
      );
    }

    @Override
    public String visitNotExpression(final NotExpression node, final Context context) {
      return "(NOT " + process(node.getValue(), context) + ")";
    }

    @Override
    public String visitComparisonExpression(
        final ComparisonExpression node,
        final Context context
    ) {
      return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(),
          context
      );
    }

    @Override
    public String visitIsNullPredicate(final IsNullPredicate node, final Context context) {
      return "(" + process(node.getValue(), context) + " IS NULL)";
    }

    @Override
    public String visitIsNotNullPredicate(final IsNotNullPredicate node, final Context context) {
      return "(" + process(node.getValue(), context) + " IS NOT NULL)";
    }

    @Override
    public String visitArithmeticUnary(
        final ArithmeticUnaryExpression node,
        final Context context
    ) {
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
        final Context context
    ) {
      return formatBinaryExpression(node.getOperator().getSymbol(), node.getLeft(), node.getRight(),
          context
      );
    }

    @Override
    public String visitLikePredicate(final LikePredicate node, final Context context) {
      return "("
          + process(node.getValue(), context)
          + " LIKE "
          + process(node.getPattern(), context)
          + node.getEscape().map(c -> " ESCAPE '" + c + "'").orElse("")
          + ')';
    }

    @Override
    public String visitCast(final Cast node, final Context context) {
      return "CAST"
          + "(" + process(node.getExpression(), context) + " AS "
          + process(node.getType(), context) + ")";
    }

    @Override
    public String visitSearchedCaseExpression(
        final SearchedCaseExpression node,
        final Context context
    ) {
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
        final Context context
    ) {
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
    public String visitBetweenPredicate(final BetweenPredicate node, final Context context) {
      return "(" + process(node.getValue(), context) + " BETWEEN "
          + process(node.getMin(), context) + " AND " + process(
          node.getMax(),
          context
      )
          + ")";
    }

    @Override
    public String visitInPredicate(final InPredicate node, final Context context) {
      return "(" + process(node.getValue(), context) + " IN " + process(
          node.getValueList(),
          context
      ) + ")";
    }

    @Override
    public String visitInListExpression(final InListExpression node, final Context context) {
      return "(" + joinExpressions(node.getValues(), context) + ")";
    }

    @Override
    public String visitLambdaExpression(
        final LambdaFunctionCall node, final Context context) {
      final StringBuilder builder = new StringBuilder();

      builder.append('(');
      Joiner.on(", ").appendTo(builder, node.getArguments());
      builder.append(") => ");
      builder.append(process(node.getBody(), context));
      return builder.toString();
    }

    private String formatBinaryExpression(
        final String operator, final Expression left, final Expression right, final Context context
    ) {
      return '(' + process(left, context) + ' ' + operator + ' ' + process(
          right,
          context
      )
          + ')';
    }

    private String joinExpressions(final List<Expression> expressions, final Context context) {
      return Joiner.on(", ").join(expressions.stream()
          .map((e) -> process(e, context))
          .iterator());
    }

    private static String formatStringLiteral(final String s) {
      return "'" + s.replace("'", "''") + "'";
    }
  }
}
