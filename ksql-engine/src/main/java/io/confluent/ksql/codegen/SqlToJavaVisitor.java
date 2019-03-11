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

package io.confluent.ksql.codegen;

import static java.lang.String.format;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.caseexpression.SearchedCaseFunction;
import io.confluent.ksql.function.udf.structfieldextractor.FetchFieldFromStruct;
import io.confluent.ksql.parser.tree.AllColumns;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.Node;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.PrimitiveType;
import io.confluent.ksql.parser.tree.QualifiedName;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.Type;
import io.confluent.ksql.schema.ksql.LogicalSchemas;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class SqlToJavaVisitor {

  public static final List<String> JAVA_IMPORTS = ImmutableList.of(
      "org.apache.kafka.connect.data.Struct",
      "io.confluent.ksql.function.udf.caseexpression.SearchedCaseFunction",
      "io.confluent.ksql.function.udf.caseexpression.SearchedCaseFunction.LazyWhenClause",
      "java.util.HashMap",
      "java.util.Map",
      "java.util.List",
      "java.util.ArrayList",
      "com.google.common.collect.ImmutableList",
      "java.util.function.Supplier");

  private final Schema schema;
  private final FunctionRegistry functionRegistry;

  private final ExpressionTypeManager expressionTypeManager;

  public SqlToJavaVisitor(final Schema schema, final FunctionRegistry functionRegistry) {
    this.schema = schema;
    this.functionRegistry = functionRegistry;
    this.expressionTypeManager =
        new ExpressionTypeManager(schema, functionRegistry);
  }

  public String process(final Expression expression) {
    return formatExpression(expression);
  }

  private String formatExpression(final Expression expression) {
    final Pair<String, Schema> expressionFormatterResult =
        new SqlToJavaVisitor.Formatter(functionRegistry).process(expression, null);
    return expressionFormatterResult.getLeft();
  }


  private class Formatter extends AstVisitor<Pair<String, Schema>, Void> {

    private final FunctionRegistry functionRegistry;
    private int functionCounter = 0;

    Formatter(final FunctionRegistry functionRegistry) {
      this.functionRegistry = functionRegistry;
    }

    @Override
    protected Pair<String, Schema> visitNode(final Node node, final Void context) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitExpression(
        final Expression node,
        final Void context
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
        final Void context
    ) {
      return new Pair<>(String.valueOf(node.getValue()), Schema.OPTIONAL_BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitStringLiteral(
        final StringLiteral node,
        final Void context
    ) {
      return new Pair<>(
          "\"" + StringEscapeUtils.escapeJava(node.getValue()) + "\"",
          Schema.OPTIONAL_STRING_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitDoubleLiteral(
        final DoubleLiteral node, final Void context) {
      return new Pair<>(Double.toString(node.getValue()), Schema.OPTIONAL_FLOAT64_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitNullLiteral(
        final NullLiteral node, final Void context) {
      return new Pair<>("null", null);
    }

    @Override
    protected Pair<String, Schema> visitQualifiedNameReference(
        final QualifiedNameReference node,
        final Void context
    ) {
      final String fieldName = formatQualifiedName(node.getName());
      final Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, fieldName);
      if (!schemaField.isPresent()) {
        throw new KsqlException("Field not found: " + fieldName);
      }
      final Schema schema = schemaField.get().schema();
      return new Pair<>(fieldName.replace(".", "_"), schema);
    }

    @Override
    protected Pair<String, Schema> visitDereferenceExpression(
        final DereferenceExpression node,
        final Void context
    ) {
      final String fieldName = node.toString();
      final Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, fieldName);
      if (!schemaField.isPresent()) {
        throw new KsqlException("Field not found: " + fieldName);
      }
      final Schema schema = schemaField.get().schema();
      return new Pair<>(fieldName.replace(".", "_"), schema);
    }

    private String formatQualifiedName(final QualifiedName name) {
      final List<String> parts = new ArrayList<>();
      for (final String part : name.getParts()) {
        parts.add(formatIdentifier(part));
      }
      return Joiner.on('.').join(parts);
    }

    protected Pair<String, Schema> visitLongLiteral(
        final LongLiteral node, final Void context) {
      return new Pair<>("Long.parseLong(\"" + node.getValue() + "\")",
          Schema.OPTIONAL_INT64_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitIntegerLiteral(final IntegerLiteral node,
        final Void context) {
      return new Pair<>("Integer.parseInt(\"" + node.getValue() + "\")",
          Schema.OPTIONAL_INT32_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitFunctionCall(
        final FunctionCall node,
        final Void context) {
      final String functionName = node.getName().getSuffix();

      final String instanceName = functionName + "_" + functionCounter++;
      final Schema functionReturnSchema = getFunctionReturnSchema(node, functionName);
      final String javaReturnType = SchemaUtil.getJavaType(functionReturnSchema).getSimpleName();
      final String arguments = node.getArguments().stream()
          .map(arg -> process(arg, context).getLeft())
          .collect(Collectors.joining(", "));
      final String codeString = "((" + javaReturnType + ") " + instanceName
          + ".evaluate(" + arguments + "))";
      return new Pair<>(codeString, functionReturnSchema);
    }

    private Schema getFunctionReturnSchema(
        final FunctionCall node,
        final String functionName) {
      if (functionName.equalsIgnoreCase(FetchFieldFromStruct.FUNCTION_NAME)) {
        return expressionTypeManager.getExpressionSchema(node);
      }
      final UdfFactory udfFactory = functionRegistry.getUdfFactory(functionName);
      final List<Schema> argumentSchemas = node.getArguments().stream()
          .map(expressionTypeManager::getExpressionSchema)
          .collect(Collectors.toList());

      return udfFactory.getFunction(argumentSchemas).getReturnType();
    }

    @Override
    protected Pair<String, Schema> visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Void context
    ) {
      if (node.getType() == LogicalBinaryExpression.Type.OR) {
        return new Pair<>(
            formatBinaryExpression(" || ", node.getLeft(), node.getRight(), context),
            Schema.OPTIONAL_BOOLEAN_SCHEMA
        );
      } else if (node.getType() == LogicalBinaryExpression.Type.AND) {
        return new Pair<>(
            formatBinaryExpression(" && ", node.getLeft(), node.getRight(), context),
            Schema.OPTIONAL_BOOLEAN_SCHEMA
        );
      }
      throw new UnsupportedOperationException(
          format("not yet implemented: %s.visit%s", getClass().getName(),
              node.getClass().getSimpleName()
          )
      );
    }

    @Override
    protected Pair<String, Schema> visitNotExpression(
        final NotExpression node, final Void context) {
      final String exprString = process(node.getValue(), context).getLeft();
      return new Pair<>("(!" + exprString + ")", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    }

    private String nullCheckPrefix(final ComparisonExpression.Type type) {
      switch (type) {
        case IS_DISTINCT_FROM:
          return "(((Object)(%1$s)) == null || ((Object)(%2$s)) == null) ? "
              + "((((Object)(%1$s)) == null ) ^ (((Object)(%2$s)) == null )) : ";
        default:
          return "(((Object)(%1$s)) == null || ((Object)(%2$s)) == null) ? false : ";
      }
    }

    private String visitStringComparisonExpression(final ComparisonExpression.Type type) {
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

    private String visitScalarComparisonExpression(final ComparisonExpression.Type type) {
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

    private String visitBooleanComparisonExpression(final ComparisonExpression.Type type) {
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
        final ComparisonExpression node,
        final Void context
    ) {
      final Pair<String, Schema> left = process(node.getLeft(), context);
      final Pair<String, Schema> right = process(node.getRight(), context);

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
      final String expr = "(" + String.format(exprFormat, left.getLeft(), right.getLeft()) + ")";
      return new Pair<>(expr, Schema.OPTIONAL_BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitCast(final Cast node, final Void context) {
      final Pair<String, Schema> expr = process(node.getExpression(), context);
      final Type sqlType = node.getType();
      if (!(sqlType instanceof PrimitiveType)) {
        throw new KsqlFunctionException("Only casts to primitive types are supported: " + sqlType);
      }

      final Schema returnType = LogicalSchemas.fromSqlTypeConverter().fromSqlType(sqlType);
      final Schema rightSchema = expr.getRight();
      if (returnType.equals(rightSchema) || rightSchema == null) {
        return new Pair<>(expr.getLeft(), returnType);
      }

      switch (sqlType.getSqlType()) {

        case STRING:
          return new Pair<>("String.valueOf(" + expr.getLeft() + ")", returnType);

        case BOOLEAN:
          return new Pair<>(getCastToBooleanString(rightSchema, expr.getLeft()), returnType);

        case INTEGER: {
          final String exprStr = getCastString(
              rightSchema,
              expr.getLeft(),
              "intValue",
              "Integer.parseInt"
          );
          return new Pair<>(exprStr, returnType);
        }

        case BIGINT: {
          final String exprStr = getCastString(
              rightSchema, expr.getLeft(),
              "longValue",
              "Long.parseLong"
          );
          return new Pair<>(exprStr, returnType);
        }

        case DOUBLE: {
          final String exprStr = getCastString(
              rightSchema,
              expr.getLeft(),
              "doubleValue",
              "Double.parseDouble"
          );
          return new Pair<>(exprStr, returnType);
        }

        default:
          throw new KsqlFunctionException("Invalid cast operation: " + sqlType);
      }
    }

    @Override
    protected Pair<String, Schema> visitIsNullPredicate(
        final IsNullPredicate node,
        final Void context
    ) {
      final Pair<String, Schema> value = process(node.getValue(), context);
      return new Pair<>("((" + value.getLeft() + ") == null )", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final Void context
    ) {
      final Pair<String, Schema> value = process(node.getValue(), context);
      return new Pair<>("((" + value.getLeft() + ") != null )", Schema.OPTIONAL_BOOLEAN_SCHEMA);
    }

    @Override
    protected Pair<String, Schema> visitArithmeticUnary(
        final ArithmeticUnaryExpression node,
        final Void context
    ) {
      final Pair<String, Schema> value = process(node.getValue(), context);
      switch (node.getSign()) {
        case MINUS:
          // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
          final String separator = value.getLeft().startsWith("-") ? " " : "";
          return new Pair<>("-" + separator + value.getLeft(), value.getRight());
        case PLUS:
          return new Pair<>("+" + value.getLeft(), value.getRight());
        default:
          throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
      }
    }

    @Override
    protected Pair<String, Schema> visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final Void context
    ) {
      final Pair<String, Schema> left = process(node.getLeft(), context);
      final Pair<String, Schema> right = process(node.getRight(), context);

      final Schema schema =
          SchemaUtil.resolveBinaryOperatorResultType(
              left.getRight().type(), right.getRight().type());

      return new Pair<>(
          "(" + left.getLeft() + " " + node.getType().getValue() + " " + right.getLeft() + ")",
          schema
      );
    }

    @Override
    protected Pair<String, Schema> visitSearchedCaseExpression(
        final SearchedCaseExpression node,
        final Void context) {
      final String functionClassName = SearchedCaseFunction.class.getSimpleName();
      final List<CaseWhenProcessed> whenClauses = node
          .getWhenClauses()
          .stream()
          .map(whenClause -> new CaseWhenProcessed(
              process(whenClause.getOperand(), context),
              process(whenClause.getResult(), context)
          ))
          .collect(Collectors.toList());
      final Schema resultSchema = whenClauses.get(0).thenProcessResult.getRight();
      final String resultSchemaString = SchemaUtil.getJavaType(resultSchema).getCanonicalName();

      final List<String> lazyWhenClause = whenClauses
          .stream()
          .map(processedWhenClause -> functionClassName + ".whenClause("
              + buildSupplierCode(
              "Boolean", processedWhenClause.whenProcessResult.getLeft())
              + ", "
              + buildSupplierCode(
              resultSchemaString, processedWhenClause.thenProcessResult.getLeft())
              + ")")
          .collect(Collectors.toList());

      final String defaultValue = node.getDefaultValue().isPresent()
          ? process(node.getDefaultValue().get(), context).getLeft()
          : "null";

      final String codeString = "((" + resultSchemaString + ")"
          + functionClassName + ".searchedCaseFunction(ImmutableList.of( "
          + StringUtils.join(lazyWhenClause, ", ") + "),"
          + buildSupplierCode(resultSchemaString, defaultValue)
          + "))";
      return new Pair<>(codeString, resultSchema);
    }

    private String buildSupplierCode(final String typeString, final String code) {
      return " new " + Supplier.class.getSimpleName() + "<" + typeString + ">() {"
          + " @Override public " + typeString + " get() { return " + code + "; }}";
    }

    @Override
    protected Pair<String, Schema> visitLikePredicate(
        final LikePredicate node,
        final Void context
    ) {

      // For now we just support simple prefix/suffix cases only.
      final String patternString = trimQuotes(process(node.getPattern(), context).getLeft());
      final String valueString = process(node.getValue(), context).getLeft();
      if (patternString.startsWith("%")) {
        if (patternString.endsWith("%")) {
          return new Pair<>(
              "(" + valueString + ").contains(\""
                  + patternString.substring(1, patternString.length() - 1)
                  + "\")",
              Schema.OPTIONAL_STRING_SCHEMA
          );
        } else {
          return new Pair<>(
              "(" + valueString + ").endsWith(\"" + patternString.substring(1) + "\")",
              Schema.OPTIONAL_STRING_SCHEMA
          );
        }
      }

      if (patternString.endsWith("%")) {
        return new Pair<>(
            "(" + valueString + ")"
                + ".startsWith(\""
                + patternString.substring(0, patternString.length() - 1) + "\")",
            Schema.OPTIONAL_STRING_SCHEMA
        );
      }

      if (!patternString.contains("%")) {
        return new Pair<>(
            "(" + valueString + ")"
                + ".equals(\""
                + patternString + "\")",
            Schema.OPTIONAL_STRING_SCHEMA
        );
      }

      throw new UnsupportedOperationException(
          "KSQL only supports leading and trailing wildcards in LIKE expressions."
      );
    }

    @Override
    protected Pair<String, Schema> visitAllColumns(
        final AllColumns node, final Void context) {
      throw new UnsupportedOperationException();
    }

    @Override
    protected Pair<String, Schema> visitSubscriptExpression(
        final SubscriptExpression node,
        final Void context
    ) {
      final Schema internalSchema = expressionTypeManager.getExpressionSchema(node.getBase());

      final String internalSchemaJavaType =
          SchemaUtil.getJavaType(internalSchema).getCanonicalName();
      switch (internalSchema.type()) {
        case ARRAY:
          return new Pair<>(
              String.format("((%s) ((%s)%s).get((int)(%s)))",
                  SchemaUtil.getJavaType(internalSchema.valueSchema()).getSimpleName(),
                  internalSchemaJavaType,
                  process(node.getBase(), context).getLeft(),
                  process(node.getIndex(), context).getLeft()
              ),
              internalSchema.valueSchema()
          );
        case MAP:
          return new Pair<>(
              String.format("((%s) ((%s)%s).get(%s))",
                  SchemaUtil.getJavaType(internalSchema.valueSchema()).getSimpleName(),
                  internalSchemaJavaType,
                  process(node.getBase(), context).getLeft(),
                  process(node.getIndex(), context).getLeft()),
              internalSchema.valueSchema()
          );
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    protected Pair<String, Schema> visitBetweenPredicate(
        final BetweenPredicate node,
        final Void context
    ) {
      final Pair<String, Schema> value = process(node.getValue(), context);
      final Pair<String, Schema> min = process(node.getMin(), context);
      final Pair<String, Schema> max = process(node.getMax(), context);

      String expression = "(((Object) {value}) == null "
          + "|| ((Object) {min}) == null "
          + "|| ((Object) {max}) == null) "
          + "? false "
          + ": ";

      final Schema.Type type = value.getRight().type();
      switch (type) {
        case FLOAT32:
        case FLOAT64:
        case INT64:
        case INT8:
        case INT16:
        case INT32:
          expression += "{min} <= {value} && {value} <= {max}";
          break;
        case STRING:
          expression += "({value}.compareTo({min}) >= 0 && {value}.compareTo({max}) <= 0)";
          break;
        default:
          throw new KsqlException("Cannot execute BETWEEN with " + type + " values");
      }

      // note that the entire expression must be surrounded by parentheses
      // otherwise negations and other higher level operations will not work
      final String evaluation = StrSubstitutor.replace(
          "(" + expression + ")",
          ImmutableMap.of(
              "value", value.getLeft(),
              "min", min.getLeft(),
              "max", max.getLeft()),
          "{", "}");

      return new Pair<>(evaluation, Schema.OPTIONAL_BOOLEAN_SCHEMA);
    }

    private String formatBinaryExpression(
        final String operator, final Expression left, final Expression right,
        final Void context
    ) {
      return "(" + process(left, context).getLeft() + " " + operator + " "
          + process(right, context).getLeft() + ")";
    }

    private String formatIdentifier(final String s) {
      // TODO: handle escaping properly
      return s;
    }

    private String trimQuotes(final String s) {
      return s.substring(1, s.length() - 1);
    }

    private String getCastToBooleanString(final Schema schema, final String exprStr) {
      if (schema.type() == Schema.Type.STRING) {
        return "Boolean.parseBoolean(" + exprStr + ")";
      } else {
        throw new KsqlFunctionException(
            "Invalid cast operation: Cannot cast " + exprStr + " to boolean.");
      }
    }

    private String getCastString(
        final Schema schema,
        final String exprStr,
        final String javaTypeMethod,
        final String javaStringParserMethod
    ) {
      switch (schema.type()) {
        case INT32:
          return "(new Integer(" + exprStr + ")." + javaTypeMethod + "())";
        case INT64:
          return "(new Long(" + exprStr + ")." + javaTypeMethod + "())";
        case FLOAT64:
          return "(new Double(" + exprStr + ")." + javaTypeMethod + "())";
        case STRING:
          return javaStringParserMethod + "(" + exprStr + ")";

        default:
          throw new KsqlFunctionException(
              "Invalid cast operation: Cannot cast " + exprStr + " to " + schema.type() + "."
          );
      }
    }
  }

  private static final class CaseWhenProcessed {

    private final Pair<String, Schema> whenProcessResult;
    private final Pair<String, Schema> thenProcessResult;

    private CaseWhenProcessed(
        final Pair<String, Schema> whenProcessResult,
        final Pair<String, Schema> thenProcessResult
    ) {
      this.whenProcessResult = whenProcessResult;
      this.thenProcessResult = thenProcessResult;
    }
  }

}