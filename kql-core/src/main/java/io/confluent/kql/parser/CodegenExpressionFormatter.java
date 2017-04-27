/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.parser;

import com.google.common.base.Joiner;

import io.confluent.kql.function.KQLFunction;
import io.confluent.kql.function.KQLFunctionException;
import io.confluent.kql.function.KQLFunctions;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.AstVisitor;
import io.confluent.kql.parser.tree.Node;
import io.confluent.kql.parser.tree.BooleanLiteral;
import io.confluent.kql.parser.tree.StringLiteral;
import io.confluent.kql.parser.tree.BinaryLiteral;
import io.confluent.kql.parser.tree.DoubleLiteral;
import io.confluent.kql.parser.tree.DecimalLiteral;
import io.confluent.kql.parser.tree.GenericLiteral;
import io.confluent.kql.parser.tree.NullLiteral;
import io.confluent.kql.parser.tree.QualifiedNameReference;
import io.confluent.kql.parser.tree.SubscriptExpression;
import io.confluent.kql.parser.tree.SymbolReference;
import io.confluent.kql.parser.tree.DereferenceExpression;
import io.confluent.kql.parser.tree.QualifiedName;
import io.confluent.kql.parser.tree.FunctionCall;
import io.confluent.kql.parser.tree.Cast;
import io.confluent.kql.parser.tree.FieldReference;
import io.confluent.kql.parser.tree.LongLiteral;
import io.confluent.kql.parser.tree.LogicalBinaryExpression;
import io.confluent.kql.parser.tree.IsNullPredicate;
import io.confluent.kql.parser.tree.IsNotNullPredicate;
import io.confluent.kql.parser.tree.BetweenPredicate;
import io.confluent.kql.parser.tree.AllColumns;
import io.confluent.kql.parser.tree.NotExpression;
import io.confluent.kql.parser.tree.ComparisonExpression;
import io.confluent.kql.parser.tree.LikePredicate;
import io.confluent.kql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.kql.parser.tree.ArithmeticBinaryExpression;


import io.confluent.kql.util.KQLException;
import io.confluent.kql.util.Pair;
import io.confluent.kql.util.SchemaUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;

public class CodegenExpressionFormatter {

  private CodegenExpressionFormatter() {
  }

  static Schema schema;


  public static String formatExpression(final Expression expression, final Schema schema) {
    CodegenExpressionFormatter.schema = schema;
    return formatExpression(expression, true);
  }

  public static String formatExpression(final Expression expression, final boolean unmangleNames) {
    Pair<String, Schema>
        expressionFormatterResult =
        new CodegenExpressionFormatter.Formatter().process(expression, unmangleNames);
    return expressionFormatterResult.getLeft();
  }


  public static class Formatter
      extends AstVisitor<Pair<String, Schema>, Boolean> {

    @Override
    protected Pair<String, Schema> visitNode(final Node node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitExpression(final Expression node, final Boolean unmangleNames) {
      throw new UnsupportedOperationException(
          format("not yet implemented: %s.visit%s", getClass().getName(),
                 node.getClass().getSimpleName()));
    }

    @Override
    protected Pair<String, Schema> visitBooleanLiteral(final BooleanLiteral node,
                                                            final Boolean unmangleNames) {
      return new Pair<>(String.valueOf(node.getValue()), Schema.BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitStringLiteral(final StringLiteral node,
                                                           final Boolean unmangleNames) {
      return new Pair<>("\"" + node.getValue() + "\"", Schema.STRING_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitBinaryLiteral(BinaryLiteral node,
                                                           Boolean unmangleNames) {
      throw new UnsupportedOperationException();
//            return new Pair<>("X'" + node.toHexString() + "'", StringType.STRING);
    }


    @Override
    protected Pair<String, Schema> visitDoubleLiteral(DoubleLiteral node,
                                                           Boolean unmangleNames) {
      return new Pair<>(Double.toString(node.getValue()), Schema.FLOAT64_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitDecimalLiteral(DecimalLiteral node,
                                                            Boolean unmangleNames) {
      throw new UnsupportedOperationException();
//            return "DECIMAL '" + node.getValue() + "'";
    }

    @Override
    protected Pair<String, Schema> visitGenericLiteral(GenericLiteral node,
                                                            Boolean unmangleNames) {
      throw new UnsupportedOperationException();
//            return node.getType() + " " + node.getValue();
    }

    @Override
    protected Pair<String, Schema> visitNullLiteral(NullLiteral node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
//            return new Pair<>("null", StringType.STRING);
    }


    @Override
    protected Pair<String, Schema> visitQualifiedNameReference(QualifiedNameReference node,
                                                                    Boolean unmangleNames) {
      String fieldName = formatQualifiedName(node.getName());
      Field schemaField = SchemaUtil.getFieldByName(schema, fieldName);
      if (schemaField == null) {
        throw new KQLException("Field not found: " + schemaField.name());
      }
      return new Pair<>(fieldName.replace(".", "_"), schemaField.schema());
    }

    @Override
    protected Pair<String, Schema> visitSymbolReference(SymbolReference node,
                                                             Boolean context) {
      String fieldName = formatIdentifier(node.getName());
      Field schemaField = SchemaUtil.getFieldByName(schema, fieldName);
      if (schemaField == null) {
        throw new KQLException("Field not found: " + schemaField.name());
      }
      return new Pair<>(fieldName, schemaField.schema());
    }

    @Override
    protected Pair<String, Schema> visitDereferenceExpression(DereferenceExpression node,
                                                                   Boolean unmangleNames) {
      String fieldName = node.toString();
      Field schemaField = SchemaUtil.getFieldByName(schema, fieldName);
      return new Pair<>(fieldName.replace(".", "_"), schemaField.schema());
//            String baseString = process(node.getBase(), unmangleNames);
//            return baseString + "." + formatIdentifier(node.getFieldName());
    }

    private static String formatQualifiedName(QualifiedName name) {
      List<String> parts = new ArrayList<>();
      for (String part : name.getParts()) {
        parts.add(formatIdentifier(part));
      }
      return Joiner.on('.').join(parts);
    }

    @Override
    public Pair<String, Schema> visitFieldReference(FieldReference node,
                                                         Boolean unmangleNames) {
      throw new UnsupportedOperationException();
      // add colon so this won't parse
//            return ":input(" + node.getFieldIndex() + ")";
    }

    protected Pair<String, Schema> visitLongLiteral(LongLiteral node, Boolean unmangleNames) {
      return new Pair<>("Long.parseLong(\"" + node.getValue() + "\")", Schema.INT64_SCHEMA);
    }


    @Override
    protected Pair<String, Schema> visitFunctionCall(FunctionCall node,
                                                          Boolean unmangleNames) {
      StringBuilder builder = new StringBuilder();
      String name = node.getName().getSuffix();
      KQLFunction kqlFunction = KQLFunctions.getFunction(name);
      String javaReturnType = SchemaUtil.getJavaType(kqlFunction.getReturnType()).getSimpleName();
      builder.append("(" + javaReturnType + ") " + name + ".evaluate(");
      boolean addComma = false;
      for (Expression argExpr:node.getArguments()) {
        Pair<String, Schema> processedArg = process(argExpr, unmangleNames);
        if (addComma) {
          builder.append(" , ");
        } else {
          addComma = true;
        }
        builder.append(processedArg.getLeft());
      }
      builder.append(")");

      return new Pair<>(builder.toString(), kqlFunction.getReturnType());
    }

    @Override
    protected Pair<String, Schema> visitLogicalBinaryExpression(LogicalBinaryExpression node,
                                                                     Boolean unmangleNames) {
      if (node.getType() == LogicalBinaryExpression.Type.OR) {
        return new Pair<>(
            formatBinaryExpression(" || ", node.getLeft(), node.getRight(), unmangleNames),
            Schema.BOOLEAN_SCHEMA);
      } else if (node.getType() == LogicalBinaryExpression.Type.AND) {
        return new Pair<>(
            formatBinaryExpression(" && ", node.getLeft(), node.getRight(), unmangleNames),
            Schema.BOOLEAN_SCHEMA);
      }
      throw new UnsupportedOperationException(
          format("not yet implemented: %s.visit%s", getClass().getName(),
                 node.getClass().getSimpleName()));
    }

    @Override
    protected Pair<String, Schema> visitNotExpression(NotExpression node,
                                                           Boolean unmangleNames) {
//            throw new UnsupportedOperationException();
//            return "(! " + process(node.getValue(), unmangleNames) + ")";
      String exprString = process(node.getValue(), unmangleNames).getLeft();
      return new Pair<>("(!" + exprString + ")", Schema.BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitComparisonExpression(ComparisonExpression node,
                                                                  Boolean unmangleNames) {
      Pair<String, Schema> left = process(node.getLeft(), unmangleNames);
      Pair<String, Schema> right = process(node.getRight(), unmangleNames);
      if ((left.getRight() == Schema.STRING_SCHEMA) || (right.getRight() == Schema.STRING_SCHEMA)) {
        if ("=".equals(node.getType().getValue())) {
          return new Pair<>("(" + left.getLeft() + ".equals(" + right.getLeft() + "))",
                            Schema.BOOLEAN_SCHEMA);
        } else if ("<>".equals(node.getType().getValue())) {
          return new Pair<>(" (!" + left.getLeft() + ".equals(" + right.getLeft() + "))",
                            Schema.BOOLEAN_SCHEMA);
        }
      }
      String typeStr = node.getType().getValue();
      if ("=".equals(typeStr)) {
        typeStr = "==";
      } else if ("<>".equals(typeStr)) {
        typeStr = "!=";
      }
      return new Pair<>("(" + left.getLeft() + " " + typeStr + " " + right.getLeft() + ")",
                        Schema.BOOLEAN_SCHEMA);
    }

    protected Pair<String, Schema> visitCast(Cast node, Boolean context) {
      Pair<String, Schema> expr = process(node.getExpression(), context);
      String returnTypeStr = node.getType();
      Schema returnType = SchemaUtil.getTypeSchema(returnTypeStr);

      switch (returnTypeStr) {

        case "STRING":
          return new Pair<>("String.valueOf(" + expr.getLeft() + ")", returnType);

        case "BOOLEAN":
          return new Pair<>(" ((Boolean)" + expr.getLeft() + ")", returnType);

        case "INTEGER": {
          String exprStr;
//          switch (expr.getRight()) {
//            case STRING:
//              exprStr = "Integer.parseInt(" + expr.getLeft() + ")";
//              break;
//            case INT32:
//              exprStr = expr.getLeft();
//              break;
//            case INT64:
//              exprStr = "((Long)(" + expr.getLeft() + "))";
//              break;
//            case FLOAT64:
//              exprStr = "(" + expr.getLeft() + ").intValue()";
//              break;
//            default:
//              throw new KQLFunctionException("Invalid cast operation: Cannot cast "
//                  + expr.getLeft() + " to " + returnTypeStr);
//          }
//          return new Pair<>(exprStr, returnType);

          Schema rightSchema = expr.getRight();
          if (rightSchema == Schema.STRING_SCHEMA) {
            exprStr = "Integer.parseInt(" + expr.getLeft() + ")";
          } else if (rightSchema == Schema.INT32_SCHEMA) {
            exprStr = expr.getLeft();
          } else if (rightSchema == Schema.INT64_SCHEMA) {
            exprStr = "((Long)(" + expr.getLeft() + "))";
          } else if (rightSchema == Schema.FLOAT64_SCHEMA) {
            exprStr = "(" + expr.getLeft() + ").intValue()";
          } else {
            throw new KQLFunctionException(
                "Invalid cast operation: Cannot cast " + expr.getLeft() + " to " + returnTypeStr);
          }
          return new Pair<>(exprStr, returnType);
        }

        case "BIGINT": {
          String exprStr;

          Schema rightSchema = expr.getRight();
          if (rightSchema == Schema.STRING_SCHEMA) {
            exprStr = "Long.parseLong(" + expr.getLeft() + ")";
          } else if (rightSchema == Schema.INT32_SCHEMA) {
            exprStr = "((Long)(" + expr.getLeft() + "))";
          } else if (rightSchema == Schema.INT64_SCHEMA) {
            exprStr = expr.getLeft();
          } else if (rightSchema == Schema.FLOAT64_SCHEMA) {
            exprStr = "(" + expr.getLeft() + ").longValue()";
          } else {
            throw new KQLFunctionException("Invalid cast operation: Cannot cast " + expr.getLeft() + " to " + returnTypeStr);
          }
          return new Pair<>(exprStr, returnType);
        }

        case "DOUBLE": {
          String exprStr;

//          switch (expr.getRight()) {
//            case STRING:
//              exprStr = "Double.parseDouble(" + expr.getLeft() + ")";
//              break;
//            case INT32:
//              exprStr = "((Double)(" + expr.getLeft() + "))";
//              break;
//            case INT64:
//              exprStr = "((Long)(" + expr.getLeft() + "))";
//              break;
//            case FLOAT64:
//              exprStr = expr.getLeft();
//              break;
//            default:
//              throw new KQLFunctionException("Invalid cast operation: Cannot cast "
//                  + expr.getLeft() + " to " + returnTypeStr);
//          }
//          return new Pair<>(exprStr, returnType);
          Schema rightSchema = expr.getRight();
          if (rightSchema == Schema.STRING_SCHEMA) {
            exprStr = "Double.parseDouble(" + expr.getLeft() + ")";
          } else if (rightSchema == Schema.INT32_SCHEMA) {
            exprStr = "((Double)(" + expr.getLeft() + "))";
          } else if (rightSchema == Schema.INT64_SCHEMA) {
            exprStr = "((Double)(" + expr.getLeft() + "))";
          } else if (rightSchema == Schema.FLOAT64_SCHEMA) {
            exprStr = expr.getLeft();
          } else {
            throw new KQLFunctionException("Invalid cast operation: Cannot cast " + expr.getLeft() + " to " + returnTypeStr);
          }
          return new Pair<>(exprStr, returnType);

        }

        default:
          throw new KQLFunctionException("Invalid cast operation: " + returnTypeStr);
      }
    }

    @Override
    protected Pair<String, Schema> visitIsNullPredicate(IsNullPredicate node,
                                                             Boolean unmangleNames) {
      Pair<String, Schema> value = process(node.getValue(), unmangleNames);
      return new Pair<>("((" + value.getLeft() + ") == null )", Schema.BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitIsNotNullPredicate(IsNotNullPredicate node,
                                                                Boolean unmangleNames) {
      Pair<String, Schema> value = process(node.getValue(), unmangleNames);
      return new Pair<>("((" + value.getLeft() + ") != null )", Schema.BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitArithmeticUnary(ArithmeticUnaryExpression node,
                                                             Boolean unmangleNames) {
      throw new UnsupportedOperationException();
//            String value = process(node.getValue(), unmangleNames);
//
//            switch (node.getSign()) {
//                case MINUS:
//                    // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
//                    String separator = value.startsWith("-") ? " " : "";
//                    return "-" + separator + value;
//                case PLUS:
//                    return "+" + value;
//                default:
//                    throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
//            }
    }

    @Override
    protected Pair<String, Schema> visitArithmeticBinary(ArithmeticBinaryExpression node,
                                                              Boolean unmangleNames) {
      Pair<String, Schema> left = process(node.getLeft(), unmangleNames);
      Pair<String, Schema> right = process(node.getRight(), unmangleNames);
      return new Pair<>(
          "(" + left.getLeft() + " " + node.getType().getValue() + " " + right.getLeft() + ")",
          Schema.FLOAT64_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitLikePredicate(LikePredicate node,
                                                           Boolean unmangleNames) {

      // For now we just support simple prefix/suffix cases only.
      String paternString = process(node.getPattern(), true).getLeft().substring(1);
      paternString = paternString.substring(0, paternString.length() - 1);
      String valueString = process(node.getValue(), true).getLeft();

      if (paternString.startsWith("%")) {
        if (paternString.endsWith("%")) {
          return new Pair<>("(" + valueString + ").contains(\"" + paternString.substring(1,
                                                                                  paternString
                                                                                      .length() - 1)
                            + "\")",
                            Schema
              .STRING_SCHEMA);
        } else {
          return new Pair<>("(" + valueString + ").endsWith(\"" + paternString.substring(1) +
                            "\")", Schema
              .STRING_SCHEMA);
        }
      }

      if (paternString.endsWith("%")) {
        return new Pair<>("(" + valueString + ").startsWith(\"" + paternString.substring(0,
                                                                                         paternString
            .length() - 1) + "\")",
                          Schema
            .STRING_SCHEMA);
      }

      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitAllColumns(AllColumns node, Boolean unmangleNames) {
      throw new UnsupportedOperationException();
//            if (node.getPrefix().isPresent()) {
//                return node.getPrefix().get() + ".*";
//            }
//
//            return "*";
    }

    @Override
    protected Pair<String, Schema> visitSubscriptExpression(SubscriptExpression node, Boolean unmangleNames) {
      String arrayBaseName = node.getBase().toString();
      Field schemaField = SchemaUtil.getFieldByName(schema, arrayBaseName);
      if (schemaField.schema().type() == Schema.Type.ARRAY) {
        return new Pair<>(process(node.getBase(), unmangleNames).getLeft() + "[(int)(" + process(node
                                                                                                     .getIndex(),
                                                                                                 unmangleNames).getLeft() + ""
                          + ")]", schema);
      } else if (schemaField.schema().type() == Schema.Type.MAP) {
        return new Pair<>("(" + SchemaUtil.getJavaCastString(schemaField.schema().valueSchema())
                          + process(node.getBase(), unmangleNames).getLeft() + ".get"
                          + "(" + process(node.getIndex(), unmangleNames).getLeft() + "))", schema);
      }
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitBetweenPredicate(BetweenPredicate node,
                                                              Boolean unmangleNames) {
      throw new UnsupportedOperationException();
//            return "(" + process(node.getValue(), unmangleNames) + " BETWEEN " +
//                    process(node.getMin(), unmangleNames) + " AND " + process(node.getMax(), unmangleNames) + ")";
    }

    private String formatBinaryExpression(String operator, Expression left, Expression right,
                                          boolean unmangleNames) {
      return "(" + process(left, unmangleNames).getLeft() + " " + operator + " " + process(right,
                                                                                           unmangleNames)
          .getLeft() + ")";
    }

    private static String formatIdentifier(String s) {
      // TODO: handle escaping properly
//            return '"' + s + '"';
      return s;
    }

    private String joinExpressions(List<Expression> expressions, boolean unmangleNames) {
      return Joiner.on(", ").join(expressions.stream()
                                      .map((e) -> process(e, unmangleNames))
                                      .iterator());
    }
  }


}
