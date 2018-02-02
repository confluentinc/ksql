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

package io.confluent.ksql.parser;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BinaryLiteral;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DecimalLiteral;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.ExistsPredicate;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.Extract;
import io.confluent.ksql.parser.tree.FieldReference;
import io.confluent.ksql.parser.tree.FrameBound;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GenericLiteral;
import io.confluent.ksql.parser.tree.GroupingElement;
import io.confluent.ksql.parser.tree.InListExpression;
import io.confluent.ksql.parser.tree.InPredicate;
import io.confluent.ksql.parser.tree.IntervalLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LambdaExpression;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullIfExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.Row;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.SimpleCaseExpression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubqueryExpression;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.SymbolReference;
import io.confluent.ksql.parser.tree.TimeLiteral;
import io.confluent.ksql.parser.tree.TimestampLiteral;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.parser.tree.Window;
import io.confluent.ksql.parser.tree.WindowFrame;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class ExpressionFormatter {

  private ExpressionFormatter() {
  }

  public static String formatExpression(Expression expression) {
    return formatExpression(expression, true);
  }

  public static String formatExpression(Expression expression, boolean unmangleNames) {
    return new Formatter().process(expression, unmangleNames);
  }

  public static class Formatter
          extends AstVisitor<String, Boolean> {

    @Override
    protected String visitNode(Node node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected String visitRow(Row node, Boolean unmangleNames) {
      return "ROW (" + Joiner.on(", ").join(node.getItems().stream()
              .map((child) -> process(child, unmangleNames))
              .collect(toList())) + ")";
    }

    @Override
    protected String visitExpression(Expression node, Boolean unmangleNames) {
      throw new UnsupportedOperationException(
              format("not yet implemented: %s.visit%s", getClass().getName(),
                      node.getClass().getSimpleName()));
    }

    @Override
    protected String visitExtract(Extract node, Boolean unmangleNames) {
      return "EXTRACT(" + node.getField() + " FROM " + process(node.getExpression(), unmangleNames)
              + ")";
    }

    @Override
    protected String visitBooleanLiteral(BooleanLiteral node, Boolean unmangleNames) {
      return String.valueOf(node.getValue());
    }

    @Override
    protected String visitStringLiteral(StringLiteral node, Boolean unmangleNames) {
      return formatStringLiteral(node.getValue());
    }

    @Override
    protected String visitBinaryLiteral(BinaryLiteral node, Boolean unmangleNames) {
      return "X'" + node.toHexString() + "'";
    }

    @Override
    protected String visitSubscriptExpression(SubscriptExpression node, Boolean unmangleNames) {
      return SqlFormatter.formatSql(node.getBase(), unmangleNames) + "[" + SqlFormatter
              .formatSql(node.getIndex(), unmangleNames) + "]";
    }

    @Override
    protected String visitLongLiteral(LongLiteral node, Boolean unmangleNames) {
      return Long.toString(node.getValue());
    }

    @Override
    protected String visitDoubleLiteral(DoubleLiteral node, Boolean unmangleNames) {
      return Double.toString(node.getValue());
    }

    @Override
    protected String visitDecimalLiteral(DecimalLiteral node, Boolean unmangleNames) {
      return "DECIMAL '" + node.getValue() + "'";
    }

    @Override
    protected String visitGenericLiteral(GenericLiteral node, Boolean unmangleNames) {
      return node.getType() + " " + formatStringLiteral(node.getValue());
    }

    @Override
    protected String visitTimeLiteral(TimeLiteral node, Boolean unmangleNames) {
      return "TIME '" + node.getValue() + "'";
    }

    @Override
    protected String visitTimestampLiteral(TimestampLiteral node, Boolean unmangleNames) {
      return "TIMESTAMP '" + node.getValue() + "'";
    }

    @Override
    protected String visitNullLiteral(NullLiteral node, Boolean unmangleNames) {
      return "null";
    }

    @Override
    protected String visitIntervalLiteral(IntervalLiteral node, Boolean unmangleNames) {
      String sign = (node.getSign() == IntervalLiteral.Sign.NEGATIVE) ? "- " : "";
      StringBuilder builder = new StringBuilder()
              .append("INTERVAL ")
              .append(sign)
              .append(" '").append(node.getValue()).append("' ")
              .append(node.getStartField());

      if (node.getEndField().isPresent()) {
        builder.append(" TO ").append(node.getEndField().get());
      }
      return builder.toString();
    }

    @Override
    protected String visitSubqueryExpression(SubqueryExpression node, Boolean unmangleNames) {
      return "(" + SqlFormatter.formatSql(node.getQuery(), unmangleNames) + ")";
    }

    @Override
    protected String visitExists(ExistsPredicate node, Boolean unmangleNames) {
      return "(EXISTS (" + SqlFormatter.formatSql(node.getSubquery(), unmangleNames) + "))";
    }

    @Override
    protected String visitQualifiedNameReference(QualifiedNameReference node,
                                                 Boolean unmangleNames) {
      return formatQualifiedName(node.getName());
    }

    @Override
    protected String visitSymbolReference(SymbolReference node, Boolean context) {
      return formatIdentifier(node.getName());
    }

    @Override
    protected String visitDereferenceExpression(DereferenceExpression node, Boolean unmangleNames) {
      String baseString = process(node.getBase(), unmangleNames);
      return baseString + "." + formatIdentifier(node.getFieldName());
    }

    private static String formatQualifiedName(QualifiedName name) {
      List<String> parts = new ArrayList<>();
      for (String part : name.getParts()) {
        parts.add(formatIdentifier(part));
      }
      return Joiner.on('.').join(parts);
    }

    @Override
    public String visitFieldReference(FieldReference node, Boolean unmangleNames) {
      // add colon so this won't parse
      return ":input(" + node.getFieldIndex() + ")";
    }

    @Override
    protected String visitFunctionCall(FunctionCall node, Boolean unmangleNames) {
      StringBuilder builder = new StringBuilder();

      String arguments = joinExpressions(node.getArguments(), unmangleNames);
      if (node.getArguments().isEmpty() && "COUNT".equals(node.getName().getSuffix())) {
        arguments = "*";
      }
      if (node.isDistinct()) {
        arguments = "DISTINCT " + arguments;
      }

      builder.append(formatQualifiedName(node.getName()))
              .append('(').append(arguments).append(')');

      if (node.getWindow().isPresent()) {
        builder.append(" OVER ").append(visitWindow(node.getWindow().get(), unmangleNames));
      }

      return builder.toString();
    }

    @Override
    protected String visitLambdaExpression(LambdaExpression node, Boolean unmangleNames) {
      StringBuilder builder = new StringBuilder();

      builder.append('(');
      Joiner.on(", ").appendTo(builder, node.getArguments());
      builder.append(") -> ");
      builder.append(process(node.getBody(), unmangleNames));
      return builder.toString();
    }

    @Override
    protected String visitLogicalBinaryExpression(LogicalBinaryExpression node,
                                                  Boolean unmangleNames) {
      return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight(),
              unmangleNames);
    }

    @Override
    protected String visitNotExpression(NotExpression node, Boolean unmangleNames) {
      return "(NOT " + process(node.getValue(), unmangleNames) + ")";
    }

    @Override
    protected String visitComparisonExpression(ComparisonExpression node, Boolean unmangleNames) {
      return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(),
              unmangleNames);
    }

    @Override
    protected String visitIsNullPredicate(IsNullPredicate node, Boolean unmangleNames) {
      return "(" + process(node.getValue(), unmangleNames) + " IS NULL)";
    }

    @Override
    protected String visitIsNotNullPredicate(IsNotNullPredicate node, Boolean unmangleNames) {
      return "(" + process(node.getValue(), unmangleNames) + " IS NOT NULL)";
    }

    @Override
    protected String visitNullIfExpression(NullIfExpression node, Boolean unmangleNames) {
      return "NULLIF(" + process(node.getFirst(), unmangleNames) + ", " + process(node.getSecond(),
              unmangleNames)
              + ')';
    }

    @Override
    protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Boolean unmangleNames) {
      String value = process(node.getValue(), unmangleNames);

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
    protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Boolean unmangleNames) {
      return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight(),
              unmangleNames);
    }

    @Override
    protected String visitLikePredicate(LikePredicate node, Boolean unmangleNames) {
      StringBuilder builder = new StringBuilder();

      builder.append('(')
              .append(process(node.getValue(), unmangleNames))
              .append(" LIKE ")
              .append(process(node.getPattern(), unmangleNames));

      if (node.getEscape() != null) {
        builder.append(" ESCAPE ")
                .append(process(node.getEscape(), unmangleNames));
      }

      builder.append(')');

      return builder.toString();
    }

    @Override
    protected String visitAllColumns(AllColumns node, Boolean unmangleNames) {
      if (node.getPrefix().isPresent()) {
        return node.getPrefix().get() + ".*";
      }

      return "*";
    }

    @Override
    public String visitCast(Cast node, Boolean unmangleNames) {
      return (node.isSafe() ? "TRY_CAST" : "CAST")
              + "(" + process(node.getExpression(), unmangleNames) + " AS " + node.getType() + ")";
    }

    @Override
    protected String visitSearchedCaseExpression(SearchedCaseExpression node,
                                                 Boolean unmangleNames) {
      ImmutableList.Builder<String> parts = ImmutableList.builder();
      parts.add("CASE");
      for (WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, unmangleNames));
      }

      node.getDefaultValue()
              .ifPresent((value) -> parts.add("ELSE").add(process(value, unmangleNames)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    protected String visitSimpleCaseExpression(SimpleCaseExpression node, Boolean unmangleNames) {
      ImmutableList.Builder<String> parts = ImmutableList.builder();

      parts.add("CASE")
              .add(process(node.getOperand(), unmangleNames));

      for (WhenClause whenClause : node.getWhenClauses()) {
        parts.add(process(whenClause, unmangleNames));
      }

      node.getDefaultValue()
              .ifPresent((value) -> parts.add("ELSE").add(process(value, unmangleNames)));

      parts.add("END");

      return "(" + Joiner.on(' ').join(parts.build()) + ")";
    }

    @Override
    protected String visitWhenClause(WhenClause node, Boolean unmangleNames) {
      return "WHEN " + process(node.getOperand(), unmangleNames) + " THEN " + process(
              node.getResult(), unmangleNames);
    }

    @Override
    protected String visitBetweenPredicate(BetweenPredicate node, Boolean unmangleNames) {
      return "(" + process(node.getValue(), unmangleNames) + " BETWEEN "
              + process(node.getMin(), unmangleNames) + " AND " + process(node.getMax(),
              unmangleNames)
              + ")";
    }

    @Override
    protected String visitInPredicate(InPredicate node, Boolean unmangleNames) {
      return "(" + process(node.getValue(), unmangleNames) + " IN " + process(node.getValueList(),
              unmangleNames) + ")";
    }

    @Override
    protected String visitInListExpression(InListExpression node, Boolean unmangleNames) {
      return "(" + joinExpressions(node.getValues(), unmangleNames) + ")";
    }

    @Override
    public String visitWindow(Window node, Boolean unmangleNames) {

      return node.toString();
    }

    @Override
    public String visitWindowFrame(WindowFrame node, Boolean unmangleNames) {
      StringBuilder builder = new StringBuilder();

      builder.append(node.getType().toString()).append(' ');

      if (node.getEnd().isPresent()) {
        builder.append("BETWEEN ")
                .append(process(node.getStart(), unmangleNames))
                .append(" AND ")
                .append(process(node.getEnd().get(), unmangleNames));
      } else {
        builder.append(process(node.getStart(), unmangleNames));
      }

      return builder.toString();
    }

    @Override
    public String visitFrameBound(FrameBound node, Boolean unmangleNames) {
      switch (node.getType()) {
        case UNBOUNDED_PRECEDING:
          return "UNBOUNDED PRECEDING";
        case PRECEDING:
          return process(node.getValue().get(), unmangleNames) + " PRECEDING";
        case CURRENT_ROW:
          return "CURRENT ROW";
        case FOLLOWING:
          return process(node.getValue().get(), unmangleNames) + " FOLLOWING";
        case UNBOUNDED_FOLLOWING:
          return "UNBOUNDED FOLLOWING";
        default:
          throw new IllegalArgumentException("unhandled type: " + node.getType());
      }
    }

    private String formatBinaryExpression(String operator, Expression left, Expression right,
                                          boolean unmangleNames) {
      return '(' + process(left, unmangleNames) + ' ' + operator + ' ' + process(right,
              unmangleNames)
              + ')';
    }

    private String joinExpressions(List<Expression> expressions, boolean unmangleNames) {
      return Joiner.on(", ").join(expressions.stream()
              .map((e) -> process(e, unmangleNames))
              .iterator());
    }

    private static String formatIdentifier(String s) {
      // TODO: handle escaping properly
      return s;
    }
  }

  public static String formatStringLiteral(String s) {
    return "'" + s.replace("'", "''") + "'";
  }


  public static String formatGroupBy(List<GroupingElement> groupingElements) {
    ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

    for (GroupingElement groupingElement : groupingElements) {
      resultStrings.add(groupingElement.format());
    }
    return Joiner.on(", ").join(resultStrings.build());
  }

  public static String formatGroupingSet(Set<Expression> groupingSet) {
    return format("(%s)", Joiner.on(", ").join(groupingSet.stream()
            .map(ExpressionFormatter::formatExpression)
            .iterator()));
  }

}
