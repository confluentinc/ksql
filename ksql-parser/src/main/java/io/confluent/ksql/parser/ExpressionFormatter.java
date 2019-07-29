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

package io.confluent.ksql.parser;

import static java.lang.String.format;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DecimalLiteral;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.ExpressionVisitor;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.ParserUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public final class ExpressionFormatter {

  private ExpressionFormatter() {
  }

  public static String formatExpression(final Expression expression) {
    return formatExpression(expression, true);
  }

  public static String formatExpression(final Expression expression, final boolean unmangleNames) {
    return new Formatter().process(expression, unmangleNames);
  }

  public static class Formatter implements ExpressionVisitor<String, Boolean> {
    @Override
    public String visitType(final Type node, final Boolean context) {
      return node.getSqlType().toString(FormatOptions.of(ParserUtil::isReservedIdentifier));
    }

    @Override
    public String visitBooleanLiteral(final BooleanLiteral node, final Boolean unmangleNames) {
      return String.valueOf(node.getValue());
    }

    @Override
    public String visitStringLiteral(final StringLiteral node, final Boolean unmangleNames) {
      return formatStringLiteral(node.getValue());
    }

    @Override
    public String visitSubscriptExpression(
        final SubscriptExpression node,
        final Boolean unmangleNames) {
      return process(node.getBase(), unmangleNames)
          + "[" + process(node.getIndex(), unmangleNames) + "]";
    }

    @Override
    public String visitLongLiteral(final LongLiteral node, final Boolean unmangleNames) {
      return Long.toString(node.getValue());
    }

    @Override
    public String visitIntegerLiteral(final IntegerLiteral node, final Boolean unmangleNames) {
      return Integer.toString(node.getValue());
    }

    @Override
    public String visitDoubleLiteral(final DoubleLiteral node, final Boolean unmangleNames) {
      return Double.toString(node.getValue());
    }

    @Override
    public String visitDecimalLiteral(final DecimalLiteral node, final Boolean unmangleNames) {
      return "DECIMAL '" + node.getValue() + "'";
    }

    @Override
    public String visitTimeLiteral(final TimeLiteral node, final Boolean unmangleNames) {
      return "TIME '" + node.getValue() + "'";
    }

    @Override
    public String visitTimestampLiteral(
        final TimestampLiteral node,
        final Boolean unmangleNames) {
      return "TIMESTAMP '" + node.getValue() + "'";
    }

    @Override
    public String visitNullLiteral(final NullLiteral node, final Boolean unmangleNames) {
      return "null";
    }

    @Override
    public String visitQualifiedNameReference(final QualifiedNameReference node,
                                                 final Boolean unmangleNames) {
      return formatQualifiedName(node.getName());
    }

    @Override
    public String visitDereferenceExpression(
        final DereferenceExpression node,
        final Boolean unmangleNames) {
      final String baseString = process(node.getBase(), unmangleNames);
      if (node.getBase() instanceof QualifiedNameReference) {
        return baseString + KsqlConstants.DOT + formatIdentifier(node.getFieldName());
      }
      return baseString + KsqlConstants.STRUCT_FIELD_REF + formatIdentifier(node.getFieldName());
    }

    private static String formatQualifiedName(final QualifiedName name) {
      final List<String> parts = new ArrayList<>();
      for (final String part : name.getParts()) {
        parts.add(formatIdentifier(part));
      }
      return Joiner.on(KsqlConstants.DOT).join(parts);
    }

    @Override
    public String visitFunctionCall(final FunctionCall node, final Boolean unmangleNames) {
      final StringBuilder builder = new StringBuilder();

      String arguments = joinExpressions(node.getArguments(), unmangleNames);
      if (node.getArguments().isEmpty() && "COUNT".equals(node.getName().getSuffix())) {
        arguments = "*";
      }

      builder.append(formatQualifiedName(node.getName()))
              .append('(').append(arguments).append(')');

      return builder.toString();
    }

    @Override
    public String visitLogicalBinaryExpression(final LogicalBinaryExpression node,
                                                  final Boolean unmangleNames) {
      return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight(),
              unmangleNames);
    }

    @Override
    public String visitNotExpression(final NotExpression node, final Boolean unmangleNames) {
      return "(NOT " + process(node.getValue(), unmangleNames) + ")";
    }

    @Override
    public String visitComparisonExpression(
        final ComparisonExpression node,
        final Boolean unmangleNames) {
      return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(),
              unmangleNames);
    }

    @Override
    public String visitIsNullPredicate(final IsNullPredicate node, final Boolean unmangleNames) {
      return "(" + process(node.getValue(), unmangleNames) + " IS NULL)";
    }

    @Override
    public String visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final Boolean unmangleNames) {
      return "(" + process(node.getValue(), unmangleNames) + " IS NOT NULL)";
    }

    @Override
    public String visitArithmeticUnary(
        final ArithmeticUnaryExpression node,
        final Boolean unmangleNames) {
      final String value = process(node.getValue(), unmangleNames);

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
        final Boolean unmangleNames) {
      return formatBinaryExpression(node.getOperator().getSymbol(), node.getLeft(), node.getRight(),
              unmangleNames);
    }

    @Override
    public String visitLikePredicate(final LikePredicate node, final Boolean unmangleNames) {
      return "("
          + process(node.getValue(), unmangleNames)
          + " LIKE "
          + process(node.getPattern(), unmangleNames)
          + ')';
    }

    @Override
    public String visitCast(final Cast node, final Boolean unmangleNames) {
      return "CAST"
              + "(" + process(node.getExpression(), unmangleNames) + " AS " + node.getType() + ")";
    }

    @Override
    public String visitSearchedCaseExpression(final SearchedCaseExpression node,
                                                 final Boolean unmangleNames) {
      final ImmutableList.Builder<String> parts = ImmutableList.builder();
      parts.add("CASE");
      for (final WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, unmangleNames));
      }

      node.getDefaultValue()
              .ifPresent((value) -> parts.add("ELSE").add(process(value, unmangleNames)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitSimpleCaseExpression(
        final SimpleCaseExpression node,
        final Boolean unmangleNames) {
      final ImmutableList.Builder<String> parts = ImmutableList.builder();

      parts.add("CASE")
              .add(process(node.getOperand(), unmangleNames));

      for (final WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, unmangleNames));
      }

      node.getDefaultValue()
              .ifPresent((value) -> parts.add("ELSE").add(process(value, unmangleNames)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    public String visitWhenClause(final WhenClause node, final Boolean unmangleNames) {
      return "WHEN " + process(node.getOperand(), unmangleNames) + " THEN " + process(
              node.getResult(), unmangleNames);
    }

    @Override
    public String visitBetweenPredicate(
        final BetweenPredicate node,
        final Boolean unmangleNames) {
      return "(" + process(node.getValue(), unmangleNames) + " BETWEEN "
              + process(node.getMin(), unmangleNames) + " AND " + process(node.getMax(),
              unmangleNames)
              + ")";
    }

    @Override
    public String visitInPredicate(final InPredicate node, final Boolean unmangleNames) {
      return "(" + process(node.getValue(), unmangleNames) + " IN " + process(node.getValueList(),
              unmangleNames) + ")";
    }

    @Override
    public String visitInListExpression(
        final InListExpression node,
        final Boolean unmangleNames) {
      return "(" + joinExpressions(node.getValues(), unmangleNames) + ")";
    }

    private String formatBinaryExpression(
        final String operator, final Expression left, final Expression right,
                                          final boolean unmangleNames) {
      return '(' + process(left, unmangleNames) + ' ' + operator + ' ' + process(right,
              unmangleNames)
              + ')';
    }

    private String joinExpressions(
        final List<Expression> expressions,
        final boolean unmangleNames) {
      return Joiner.on(", ").join(expressions.stream()
              .map((e) -> process(e, unmangleNames))
              .iterator());
    }

    private static String formatIdentifier(final String s) {
      // TODO: handle escaping properly
      return s;
    }
  }

  public static String formatStringLiteral(final String s) {
    return "'" + s.replace("'", "''") + "'";
  }


  public static String formatGroupBy(final List<GroupingElement> groupingElements) {
    final ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

    for (final GroupingElement groupingElement : groupingElements) {
      resultStrings.add(groupingElement.format());
    }
    return Joiner.on(", ").join(resultStrings.build());
  }

  public static String formatGroupingSet(final Set<Expression> groupingSet) {
    return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
            .map(ExpressionFormatter::formatExpression)
            .iterator()));
  }

}
