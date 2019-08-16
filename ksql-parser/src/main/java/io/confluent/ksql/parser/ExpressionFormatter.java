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
import static java.util.stream.Collectors.toList;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.Array;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DecimalLiteral;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.Expression;
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
import io.confluent.ksql.parser.tree.Map;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.Struct;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.ParserUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public final class ExpressionFormatter {

  private ExpressionFormatter() {
  }

  public static String formatExpression(final Expression expression) {
    return formatExpression(expression, true);
  }

  public static String formatExpression(final Expression expression, final boolean unmangleNames) {
    return formatExpression(expression, unmangleNames, s -> false);
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

  public static class Formatter
          extends AstVisitor<String, Context> {

    @Override
    protected String visitNode(final Node node, final Context context) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String visitPrimitiveType(final PrimitiveType node, final Context context) {
      return node.getSqlType().toString();
    }

    @Override
    protected String visitArray(final Array node, final Context context) {
      return "ARRAY<" + process(node.getItemType(), context) + ">";
    }

    @Override
    protected String visitMap(final Map node, final Context context) {
      return "MAP<VARCHAR, " + process(node.getValueType(), context) + ">";
    }

    @Override
    protected String visitStruct(final Struct node, final Context context) {
      return "STRUCT<" + Joiner.on(", ").join(node.getFields().stream()
          .map((child) ->
              ParserUtil.escapeIfLiteral(child.getName())
                  + " " + process(child.getType(), context))
          .collect(toList())) + ">";
    }

    @Override
    protected String visitExpression(final Expression node, final Context context) {
      throw new UnsupportedOperationException(
              format("not yet implemented: %s.visit%s", getClass().getName(),
                      node.getClass().getSimpleName()));
    }

    @Override
    protected String visitBooleanLiteral(final BooleanLiteral node, final Context context) {
      return String.valueOf(node.getValue());
    }

    @Override
    protected String visitStringLiteral(final StringLiteral node, final Context context) {
      return formatStringLiteral(node.getValue());
    }

    @Override
    protected String visitSubscriptExpression(
        final SubscriptExpression node,
        final Context context) {
      return SqlFormatter.formatSql(node.getBase(), context.unmangleNames) + "[" + SqlFormatter
              .formatSql(node.getIndex(), context.unmangleNames) + "]";
    }

    @Override
    protected String visitLongLiteral(final LongLiteral node, final Context context) {
      return Long.toString(node.getValue());
    }

    @Override
    protected String visitIntegerLiteral(final IntegerLiteral node, final Context context) {
      return Integer.toString(node.getValue());
    }

    @Override
    protected String visitDoubleLiteral(final DoubleLiteral node, final Context context) {
      return Double.toString(node.getValue());
    }

    @Override
    protected String visitDecimalLiteral(final DecimalLiteral node, final Context context) {
      return "DECIMAL '" + node.getValue() + "'";
    }

    @Override
    protected String visitTimeLiteral(final TimeLiteral node, final Context context) {
      return "TIME '" + node.getValue() + "'";
    }

    @Override
    protected String visitTimestampLiteral(
        final TimestampLiteral node,
        final Context context) {
      return "TIMESTAMP '" + node.getValue() + "'";
    }

    @Override
    protected String visitNullLiteral(final NullLiteral node, final Context context) {
      return "null";
    }

    @Override
    protected String visitQualifiedNameReference(final QualifiedNameReference node,
                                                 final Context context) {
      return formatQualifiedName(node.getName());
    }

    @Override
    protected String visitDereferenceExpression(
        final DereferenceExpression node,
        final Context context) {
      final String baseString = process(node.getBase(), context);
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
    protected String visitFunctionCall(final FunctionCall node, final Context context) {
      final StringBuilder builder = new StringBuilder();

      String arguments = joinExpressions(node.getArguments(), context);
      if (node.getArguments().isEmpty() && "COUNT".equals(node.getName().getSuffix())) {
        arguments = "*";
      }

      builder.append(formatQualifiedName(node.getName()))
              .append('(').append(arguments).append(')');

      return builder.toString();
    }

    @Override
    protected String visitLogicalBinaryExpression(final LogicalBinaryExpression node,
                                                  final Context context) {
      return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight(),
          context);
    }

    @Override
    protected String visitNotExpression(final NotExpression node, final Context context) {
      return "(NOT " + process(node.getValue(), context) + ")";
    }

    @Override
    protected String visitComparisonExpression(
        final ComparisonExpression node,
        final Context context) {
      return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(),
          context);
    }

    @Override
    protected String visitIsNullPredicate(final IsNullPredicate node, final Context context) {
      return "(" + process(node.getValue(), context) + " IS NULL)";
    }

    @Override
    protected String visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final Context context) {
      return "(" + process(node.getValue(), context) + " IS NOT NULL)";
    }

    @Override
    protected String visitArithmeticUnary(
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
    protected String visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final Context context) {
      return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(),
          context);
    }

    @Override
    protected String visitLikePredicate(final LikePredicate node, final Context context) {
      return "("
          + process(node.getValue(), context)
          + " LIKE "
          + process(node.getPattern(), context)
          + ')';
    }

    @Override
    protected String visitAllColumns(final AllColumns node, final Context context) {
      if (node.getPrefix().isPresent()) {
        return node.getPrefix().get() + ".*";
      }

      return "*";
    }

    @Override
    public String visitCast(final Cast node, final Context context) {
      return "CAST"
              + "(" + process(node.getExpression(), context) + " AS " + node.getType() + ")";
    }

    @Override
    protected String visitSearchedCaseExpression(final SearchedCaseExpression node,
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
    protected String visitSimpleCaseExpression(
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
    protected String visitWhenClause(final WhenClause node, final Context context) {
      return "WHEN " + process(node.getOperand(), context) + " THEN " + process(
              node.getResult(), context);
    }

    @Override
    protected String visitBetweenPredicate(
        final BetweenPredicate node,
        final Context context) {
      return "(" + process(node.getValue(), context) + " BETWEEN "
              + process(node.getMin(), context) + " AND " + process(node.getMax(),
          context)
              + ")";
    }

    @Override
    protected String visitInPredicate(final InPredicate node, final Context context) {
      return "(" + process(node.getValue(), context) + " IN " + process(node.getValueList(),
          context) + ")";
    }

    @Override
    protected String visitInListExpression(
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
