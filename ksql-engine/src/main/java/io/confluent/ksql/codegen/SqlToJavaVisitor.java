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

package io.confluent.ksql.codegen;

import com.google.common.base.Joiner;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.KsqlFunctionException;
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
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FieldReference;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.GenericLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.SymbolReference;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;

import static java.lang.String.format;

public class SqlToJavaVisitor {

  private Schema schema;
  private FunctionRegistry functionRegistry;

  public SqlToJavaVisitor(Schema schema, FunctionRegistry functionRegistry) {
    this.schema = schema;
    this.functionRegistry = functionRegistry;
  }

  public String process(final Expression expression) {
    return formatExpression(expression);
  }

  private String formatExpression(final Expression expression) {
    Pair<String, Schema> expressionFormatterResult =
        new SqlToJavaVisitor.Formatter(functionRegistry).process(expression, true);
    return expressionFormatterResult.getLeft();
  }


  public class Formatter extends AstVisitor<Pair<String, Schema>, Boolean> {

    FunctionRegistry functionRegistry;

    Formatter(FunctionRegistry functionRegistry) {
      this.functionRegistry = functionRegistry;
    }

    @Override
    protected Pair<String, Schema> visitNode(final Node node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitExpression(
        final Expression node,
        final Boolean unmangleNames
    ) {
      throw new UnsupportedOperationException(
          format(
              "not yet implemented: %s.visit%s",
              getClass().getName(),
              node.getClass().getSimpleName()
          )
      );
    }

    @Override
    protected Pair<String, Schema> visitBooleanLiteral(
        final BooleanLiteral node,
        final Boolean unmangleNames
    ) {
      return new Pair<>(String.valueOf(node.getValue()), Schema.BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitStringLiteral(
        final StringLiteral node,
        final Boolean unmangleNames
    ) {
      return new Pair<>("\"" + node.getValue() + "\"", Schema.STRING_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitBinaryLiteral(BinaryLiteral node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
    }


    @Override
    protected Pair<String, Schema> visitDoubleLiteral(DoubleLiteral node, Boolean unmangleNames) {
      return new Pair<>(Double.toString(node.getValue()), Schema.FLOAT64_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitDecimalLiteral(DecimalLiteral node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitGenericLiteral(
        GenericLiteral node,
        Boolean unmangleNames
    ) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitNullLiteral(NullLiteral node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitQualifiedNameReference(
        QualifiedNameReference node,
        Boolean unmangleNames
    ) {
      String fieldName = formatQualifiedName(node.getName());
      Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, fieldName);
      if (!schemaField.isPresent()) {
        throw new KsqlException("Field not found: " + fieldName);
      }
      return new Pair<>(fieldName.replace(".", "_"), schemaField.get().schema());
    }

    @Override
    protected Pair<String, Schema> visitSymbolReference(
        SymbolReference node,
        Boolean context
    ) {
      String fieldName = formatIdentifier(node.getName());
      Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, fieldName);
      if (!schemaField.isPresent()) {
        throw new KsqlException("Field not found: " + fieldName);
      }
      return new Pair<>(fieldName, schemaField.get().schema());
    }

    @Override
    protected Pair<String, Schema> visitDereferenceExpression(
        DereferenceExpression node,
        Boolean unmangleNames
    ) {
      String fieldName = node.toString();
      Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, fieldName);
      if (!schemaField.isPresent()) {
        throw new KsqlException("Field not found: " + fieldName);
      }
      return new Pair<>(fieldName.replace(".", "_"), schemaField.get().schema());
    }

    private String formatQualifiedName(QualifiedName name) {
      List<String> parts = new ArrayList<>();
      for (String part : name.getParts()) {
        parts.add(formatIdentifier(part));
      }
      return Joiner.on('.').join(parts);
    }

    @Override
    public Pair<String, Schema> visitFieldReference(
        FieldReference node,
        Boolean unmangleNames
    ) {
      throw new UnsupportedOperationException();
    }

    protected Pair<String, Schema> visitLongLiteral(LongLiteral node, Boolean unmangleNames) {
      return new Pair<>("Long.parseLong(\"" + node.getValue() + "\")", Schema.INT64_SCHEMA);
    }


    @Override
    protected Pair<String, Schema> visitFunctionCall(FunctionCall node, Boolean unmangleNames) {
      StringBuilder builder = new StringBuilder("(");
      String name = node.getName().getSuffix();
      KsqlFunction ksqlFunction = functionRegistry.getFunction(name);
      String javaReturnType = SchemaUtil.getJavaType(ksqlFunction.getReturnType()).getSimpleName();
      builder.append("(" + javaReturnType + ") " + name + ".evaluate(");
      boolean addComma = false;
      for (Expression argExpr : node.getArguments()) {
        Pair<String, Schema> processedArg = process(argExpr, unmangleNames);
        if (addComma) {
          builder.append(" , ");
        } else {
          addComma = true;
        }
        builder.append(processedArg.getLeft());
      }
      builder.append(")");
      builder.append(")");
      return new Pair<>(builder.toString(), ksqlFunction.getReturnType());
    }

    @Override
    protected Pair<String, Schema> visitLogicalBinaryExpression(
        LogicalBinaryExpression node,
        Boolean unmangleNames
    ) {
      if (node.getType() == LogicalBinaryExpression.Type.OR) {
        return new Pair<>(
            formatBinaryExpression(" || ", node.getLeft(), node.getRight(), unmangleNames),
            Schema.BOOLEAN_SCHEMA
        );
      } else if (node.getType() == LogicalBinaryExpression.Type.AND) {
        return new Pair<>(
            formatBinaryExpression(" && ", node.getLeft(), node.getRight(), unmangleNames),
            Schema.BOOLEAN_SCHEMA
        );
      }
      throw new UnsupportedOperationException(
          format("not yet implemented: %s.visit%s", getClass().getName(),
                 node.getClass().getSimpleName()
          )
      );
    }

    @Override
    protected Pair<String, Schema> visitNotExpression(NotExpression node, Boolean unmangleNames) {
      String exprString = process(node.getValue(), unmangleNames).getLeft();
      return new Pair<>("(!" + exprString + ")", Schema.BOOLEAN_SCHEMA);
    }

    private String nullCheckPrefix(ComparisonExpression.Type type) {
      switch (type) {
        case IS_DISTINCT_FROM:
          return "(((Object)%1$s) == null || ((Object)%2$s) == null) ? "
              + "((((Object)%1$s) == null ) ^ (((Object)%2$s) == null )) : ";
        default:
          return "(((Object)%1$s) == null || ((Object)%2$s) == null) ? false : ";
      }
    }

    private String visitStringComparisonExpression(ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "(%1$s.equals(%2$s))";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "(!%1$s.equals(%2$s))";
        case GREATER_THAN_OR_EQUAL:
        case GREATER_THAN:
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
          return "(%1$s.compareTo(%2$s) " + type.getValue() + " 0)";
        default:
          throw new KsqlException("Unexpected string comparison: " + type.getValue());
      }
    }

    private String visitScalarComparisonExpression(ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "((%1$s <= %2$s) && (%1$s >= %2$s))";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "((%1$s < %2$s) || (%1$s > %2$s))";
        case GREATER_THAN_OR_EQUAL:
        case GREATER_THAN:
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
          return "(%1$s " + type.getValue() + " %2$s)";
        default:
          throw new KsqlException("Unexpected scalar comparison: " + type.getValue());
      }
    }

    private String visitBooleanComparisonExpression(ComparisonExpression.Type type) {
      switch (type) {
        case EQUAL:
          return "(Boolean.compare(%1$s, %2$s) == 0)";
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return "(Boolean.compare(%1$s, %2$s) != 0)";
        default:
          throw new KsqlException("Unexpected boolean comparison: " + type.getValue());
      }
    }

    @Override
    protected Pair<String, Schema> visitComparisonExpression(
        ComparisonExpression node,
        Boolean unmangleNames
    ) {
      Pair<String, Schema> left = process(node.getLeft(), unmangleNames);
      Pair<String, Schema> right = process(node.getRight(), unmangleNames);

      String exprFormat = nullCheckPrefix(node.getType());
      switch (left.getRight().type()) {
        case STRING:
          exprFormat += visitStringComparisonExpression(node.getType());
          break;
        case MAP:
          throw new KsqlException("Cannot compare MAP values");
        case ARRAY:
          throw new KsqlException("Cannot compare ARRAY values");
        case BOOLEAN:
          exprFormat += visitBooleanComparisonExpression(node.getType());
          break;
        default:
          exprFormat += visitScalarComparisonExpression(node.getType());
          break;
      }
      String expr = "(" + String.format(exprFormat, left.getLeft(), right.getLeft()) + ")";
      return new Pair<>(expr, Schema.BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitCast(Cast node, Boolean context) {
      Pair<String, Schema> expr = process(node.getExpression(), context);
      String returnTypeStr = node.getType();
      Schema returnType = SchemaUtil.getTypeSchema(returnTypeStr);

      switch (returnTypeStr) {

        case "VARCHAR":
        case "STRING":
          return new Pair<>("String.valueOf(" + expr.getLeft() + ")", returnType);

        case "BOOLEAN": {
          Schema rightSchema = expr.getRight();
          return new Pair<>(getCastToBooleanString(rightSchema, expr.getLeft()), returnType);
        }

        case "INTEGER": {
          Schema rightSchema = expr.getRight();
          String exprStr = getCastString(
              rightSchema,
              expr.getLeft(),
              "intValue",
              "Integer.parseInt"
          );
          return new Pair<>(exprStr, returnType);
        }

        case "BIGINT": {
          Schema rightSchema = expr.getRight();
          String exprStr = getCastString(
              rightSchema, expr.getLeft(),
              "longValue",
              "Long.parseLong"
          );
          return new Pair<>(exprStr, returnType);
        }

        case "DOUBLE": {
          Schema rightSchema = expr.getRight();
          String exprStr = getCastString(
              rightSchema,
              expr.getLeft(),
              "doubleValue",
              "Double.parseDouble"
          );
          return new Pair<>(exprStr, returnType);
        }
        default:
          throw new KsqlFunctionException("Invalid cast operation: " + returnTypeStr);
      }
    }

    @Override
    protected Pair<String, Schema> visitIsNullPredicate(
        IsNullPredicate node,
        Boolean unmangleNames
    ) {
      Pair<String, Schema> value = process(node.getValue(), unmangleNames);
      return new Pair<>("((" + value.getLeft() + ") == null )", Schema.BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitIsNotNullPredicate(
        IsNotNullPredicate node,
        Boolean unmangleNames
    ) {
      Pair<String, Schema> value = process(node.getValue(), unmangleNames);
      return new Pair<>("((" + value.getLeft() + ") != null )", Schema.BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitArithmeticUnary(
        ArithmeticUnaryExpression node,
        Boolean unmangleNames
    ) {
      Pair<String, Schema> value = process(node.getValue(), unmangleNames);

      switch (node.getSign()) {
        case MINUS:
          // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
          String separator = value.getLeft().startsWith("-") ? " " : "";
          return new Pair<>("-" + separator + value.getLeft(), value.getRight());
        case PLUS:
          return new Pair<>("+" + value.getLeft(), value.getRight());
        default:
          throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
      }
    }

    @Override
    protected Pair<String, Schema> visitArithmeticBinary(
        ArithmeticBinaryExpression node,
        Boolean unmangleNames
    ) {
      Pair<String, Schema> left = process(node.getLeft(), unmangleNames);
      Pair<String, Schema> right = process(node.getRight(), unmangleNames);
      return new Pair<>(
          "(" + left.getLeft() + " " + node.getType().getValue() + " " + right.getLeft() + ")",
          Schema.FLOAT64_SCHEMA
      );
    }

    @Override
    protected Pair<String, Schema> visitLikePredicate(
        LikePredicate node,
        Boolean unmangleNames
    ) {

      // For now we just support simple prefix/suffix cases only.
      String paternString = process(node.getPattern(), true).getLeft().substring(1);
      paternString = paternString.substring(0, paternString.length() - 1);
      String valueString = process(node.getValue(), true).getLeft();

      if (paternString.startsWith("%")) {
        if (paternString.endsWith("%")) {
          return new Pair<>(
              "(" + valueString + ").contains(\""
              + paternString.substring(1, paternString.length() - 1)
              + "\")",
              Schema.STRING_SCHEMA
          );
        } else {
          return new Pair<>(
              "(" + valueString + ").endsWith(\"" + paternString.substring(1) + "\")",
              Schema.STRING_SCHEMA
          );
        }
      }

      if (paternString.endsWith("%")) {
        return new Pair<>(
            "(" + valueString + ")"
            + ".startsWith(\""
            + paternString.substring(0, paternString.length() - 1) + "\")",
            Schema.STRING_SCHEMA
        );
      }

      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitAllColumns(AllColumns node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitSubscriptExpression(
        SubscriptExpression node,
        Boolean unmangleNames
    ) {
      String arrayBaseName = node.getBase().toString();
      Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, arrayBaseName);
      if (!schemaField.isPresent()) {
        throw new KsqlException("Field not found: " + arrayBaseName);
      }
      if (schemaField.get().schema().type() == Schema.Type.ARRAY) {
        return new Pair<>(
            process(node.getBase(), unmangleNames).getLeft() + "[(int)("
            + process(node.getIndex(), unmangleNames).getLeft() + ")]",
            schema
        );
      } else if (schemaField.get().schema().type() == Schema.Type.MAP) {
        return new Pair<>(
            "("
            + SchemaUtil.getJavaCastString(schemaField.get().schema().valueSchema())
            + process(node.getBase(), unmangleNames).getLeft() + ".get"
            + "(" + process(node.getIndex(), unmangleNames).getLeft() + "))",
            schema
        );
      }
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitBetweenPredicate(
        BetweenPredicate node,
        Boolean unmangleNames
    ) {
      throw new UnsupportedOperationException();
    }

    private String formatBinaryExpression(
        String operator, Expression left, Expression right,
        boolean unmangleNames
    ) {
      return "(" + process(left, unmangleNames).getLeft() + " " + operator + " "
             + process(right, unmangleNames).getLeft() + ")";
    }

    private String formatIdentifier(String s) {
      // TODO: handle escaping properly
      return s;
    }

    private String joinExpressions(List<Expression> expressions, boolean unmangleNames) {
      return Joiner.on(", ").join(expressions.stream()
                                      .map((e) -> process(e, unmangleNames)).iterator());
    }

    private String getCastToBooleanString(Schema schema, String exprStr) {
      if (schema.type() == Schema.Type.BOOLEAN) {
        return exprStr;
      } else if (schema.type() == Schema.Type.STRING) {
        return "Boolean.parseBoolean(" + exprStr + ")";
      } else {
        throw new KsqlFunctionException(
            "Invalid cast operation: Cannot cast " + exprStr + " to boolean.");
      }
    }

    private String getCastString(
        Schema schema,
        String exprStr,
        String javaTypeMethod,
        String javaStringParserMethod
    ) {
      switch (schema.type()) {
        case INT32:
          if (javaTypeMethod.equals("intValue")) {
            return exprStr;
          } else {
            return "(new Integer(" + exprStr + ")." + javaTypeMethod + "())";
          }
        case INT64:
          if (javaTypeMethod.equals("longValue")) {
            return exprStr;
          } else {
            return "(new Long(" + exprStr + ")." + javaTypeMethod + "())";
          }
        case FLOAT64:
          if (javaTypeMethod.equals("doubleValue")) {
            return exprStr;
          } else {
            return "(new Double(" + exprStr + ")." + javaTypeMethod + "())";
          }
        case STRING:
          return javaStringParserMethod + "(" + exprStr + ")";

        default:
          throw new KsqlFunctionException(
              "Invalid cast operation: Cannot cast " + exprStr + " to " + schema.type() + "."
          );
      }
    }
  }
}
