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
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
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
import io.confluent.ksql.execution.expression.tree.IsNotNullPredicate;
import io.confluent.ksql.execution.expression.tree.IsNullPredicate;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.LogicalBinaryExpression;
import io.confluent.ksql.execution.expression.tree.LongLiteral;
import io.confluent.ksql.execution.expression.tree.NotExpression;
import io.confluent.ksql.execution.expression.tree.NullLiteral;
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.name.Name;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlConstants;
import java.util.List;
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
    public String visitType(Type node, Context context) {
      return node.getSqlType().toString(context.formatOptions);
    }

    @Override
    public String visitBooleanLiteral(BooleanLiteral node, Context context) {
      return String.valueOf(node.getValue());
    }

    @Override
    public String visitStringLiteral(StringLiteral node, Context context) {
      return formatStringLiteral(node.getValue());
    }

    @Override
    public String visitSubscriptExpression(SubscriptExpression node, Context context) {
      return process(node.getBase(), context)
          + "[" + process(node.getIndex(), context) + "]";
    }

    @Override
    public String visitStructExpression(CreateStructExpression exp, Context context) {
      return exp
          .getFields()
          .stream()
          .map(struct -> struct.getName() + ":=" + process(struct.getValue(), context))
          .collect(Collectors.joining(", ", "STRUCT(", ")"));
    }

    @Override
    public String visitLongLiteral(LongLiteral node, Context context) {
      return Long.toString(node.getValue());
    }

    @Override
    public String visitIntegerLiteral(IntegerLiteral node, Context context) {
      return Integer.toString(node.getValue());
    }

    @Override
    public String visitDoubleLiteral(DoubleLiteral node, Context context) {
      return Double.toString(node.getValue());
    }

    @Override
    public String visitDecimalLiteral(DecimalLiteral node, Context context) {
      return "DECIMAL '" + node.getValue() + "'";
    }

    @Override
    public String visitTimeLiteral(TimeLiteral node, Context context) {
      return "TIME '" + node.getValue() + "'";
    }

    @Override
    public String visitTimestampLiteral(TimestampLiteral node, Context context) {
      return "TIMESTAMP '" + node.getValue() + "'";
    }

    @Override
    public String visitNullLiteral(NullLiteral node, Context context) {
      return "null";
    }

    @Override
    public String visitColumnReference(ColumnReferenceExp node, Context context) {
      return node.getReference().toString(context.formatOptions);
    }

    @Override
    public String visitDereferenceExpression(DereferenceExpression node, Context context) {
      String baseString = process(node.getBase(), context);
      return baseString + KsqlConstants.STRUCT_FIELD_REF
          + context.formatOptions.escape(node.getFieldName());
    }

    private static String formatName(Name<?> name, Context context) {
      return name.toString(context.formatOptions);
    }

    @Override
    public String visitFunctionCall(FunctionCall node, Context context) {
      StringBuilder builder = new StringBuilder();

      String arguments = joinExpressions(node.getArguments(), context);
      if (node.getArguments().isEmpty() && "COUNT".equals(node.getName().name())) {
        arguments = "*";
      }

      builder.append(formatName(node.getName(), context))
          .append('(').append(arguments).append(')');

      return builder.toString();
    }

    @Override
    public String visitLogicalBinaryExpression(LogicalBinaryExpression node, Context context) {
      return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight(),
          context
      );
    }

    @Override
    public String visitNotExpression(NotExpression node, Context context) {
      return "(NOT " + process(node.getValue(), context) + ")";
    }

    @Override
    public String visitComparisonExpression(ComparisonExpression node, Context context) {
      return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(),
          context
      );
    }

    @Override
    public String visitIsNullPredicate(IsNullPredicate node, Context context) {
      return "(" + process(node.getValue(), context) + " IS NULL)";
    }

    @Override
    public String visitIsNotNullPredicate(IsNotNullPredicate node, Context context) {
      return "(" + process(node.getValue(), context) + " IS NOT NULL)";
    }

    @Override
    public String visitArithmeticUnary(ArithmeticUnaryExpression node, Context context) {
      String value = process(node.getValue(), context);

      switch (node.getSign()) {
        case MINUS:
          // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
          String separator = value.startsWith("-") ? " " : "";
          return "-" + separator + value;
        case PLUS:
          return "+" + value;
        default:
          throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
      }
    }

    @Override
    public String visitArithmeticBinary(ArithmeticBinaryExpression node, Context context) {
      return formatBinaryExpression(node.getOperator().getSymbol(), node.getLeft(), node.getRight(),
          context
      );
    }

    @Override
    public String visitLikePredicate(LikePredicate node, Context context) {
      return "("
          + process(node.getValue(), context)
          + " LIKE "
          + process(node.getPattern(), context)
          + ')';
    }

    @Override
    public String visitCast(Cast node, Context context) {
      return "CAST"
          + "(" + process(node.getExpression(), context) + " AS " + node.getType() + ")";
    }

    @Override
    public String visitSearchedCaseExpression(SearchedCaseExpression node, Context context) {
      ImmutableList.Builder<String> parts = ImmutableList.builder();
      parts.add("CASE");
      for (WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, context));
      }

      node.getDefaultValue()
          .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitSimpleCaseExpression(SimpleCaseExpression node, Context context) {
      ImmutableList.Builder<String> parts = ImmutableList.builder();

      parts.add("CASE")
          .add(process(node.getOperand(), context));

      for (WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, context));
      }

      node.getDefaultValue()
          .ifPresent((value) -> parts.add("ELSE").add(process(value, context)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitWhenClause(WhenClause node, Context context) {
      return "WHEN " + process(node.getOperand(), context) + " THEN " + process(
          node.getResult(), context);
    }

    @Override
    public String visitBetweenPredicate(BetweenPredicate node, Context context) {
      return "(" + process(node.getValue(), context) + " BETWEEN "
          + process(node.getMin(), context) + " AND " + process(
          node.getMax(),
          context
      )
          + ")";
    }

    @Override
    public String visitInPredicate(InPredicate node, Context context) {
      return "(" + process(node.getValue(), context) + " IN " + process(
          node.getValueList(),
          context
      ) + ")";
    }

    @Override
    public String visitInListExpression(InListExpression node, Context context) {
      return "(" + joinExpressions(node.getValues(), context) + ")";
    }

    private String formatBinaryExpression(
        String operator, Expression left, Expression right, Context context
    ) {
      return '(' + process(left, context) + ' ' + operator + ' ' + process(
          right,
          context
      )
          + ')';
    }

    private String joinExpressions(List<Expression> expressions, Context context) {
      return Joiner.on(", ").join(expressions.stream()
          .map((e) -> process(e, context))
          .iterator());
    }

    private static String formatStringLiteral(String s) {
      return "'" + s.replace("'", "''") + "'";
    }
  }
}
