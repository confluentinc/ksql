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

package io.confluent.ksql.execution.codegen;

import static java.lang.String.format;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multiset;
import io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
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
import io.confluent.ksql.execution.expression.tree.SearchedCaseExpression;
import io.confluent.ksql.execution.expression.tree.SimpleCaseExpression;
import io.confluent.ksql.execution.expression.tree.StringLiteral;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TimeLiteral;
import io.confluent.ksql.execution.expression.tree.TimestampLiteral;
import io.confluent.ksql.execution.expression.tree.Type;
import io.confluent.ksql.execution.expression.tree.WhenClause;
import io.confluent.ksql.execution.function.udf.structfieldextractor.FetchFieldFromStruct;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;

public class SqlToJavaVisitor {

  public static final List<String> JAVA_IMPORTS = ImmutableList.of(
      "org.apache.kafka.connect.data.Struct",
      "io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction",
      "io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction.LazyWhenClause",
      "java.util.HashMap",
      "java.util.Map",
      "java.util.List",
      "java.util.ArrayList",
      "com.google.common.collect.ImmutableList",
      "java.util.function.Supplier",
      DecimalUtil.class.getCanonicalName(),
      BigDecimal.class.getCanonicalName(),
      MathContext.class.getCanonicalName(),
      RoundingMode.class.getCanonicalName()
  );

  private static final Map<Operator, String> DECIMAL_OPERATOR_NAME = ImmutableMap
      .<Operator, String>builder()
      .put(Operator.ADD, "add")
      .put(Operator.SUBTRACT, "subtract")
      .put(Operator.MULTIPLY, "multiply")
      .put(Operator.DIVIDE, "divide")
      .put(Operator.MODULUS, "remainder")
      .build();

  private static final Map<ComparisonExpression.Type, String> SQL_COMPARE_TO_JAVA = ImmutableMap
      .<ComparisonExpression.Type, String>builder()
      .put(ComparisonExpression.Type.EQUAL, "==")
      .put(ComparisonExpression.Type.NOT_EQUAL, "!=")
      .put(ComparisonExpression.Type.IS_DISTINCT_FROM, "!=")
      .put(ComparisonExpression.Type.GREATER_THAN_OR_EQUAL, ">=")
      .put(ComparisonExpression.Type.GREATER_THAN, ">")
      .put(ComparisonExpression.Type.LESS_THAN_OR_EQUAL, "<=")
      .put(ComparisonExpression.Type.LESS_THAN, "<")
      .build();

  private final LogicalSchema schema;
  private final FunctionRegistry functionRegistry;

  private final ExpressionTypeManager expressionTypeManager;
  private final Function<FunctionName, String> funNameToCodeName;
  private final Function<ColumnRef, String> colRefToCodeName;

  public static SqlToJavaVisitor of(
      LogicalSchema schema, FunctionRegistry functionRegistry, CodeGenSpec spec
  ) {
    Multiset<FunctionName> nameCounts = HashMultiset.create();
    return new SqlToJavaVisitor(
        schema,
        functionRegistry,
        spec::getCodeName,
        name -> {
          int index = nameCounts.add(name, 1);
          return spec.getUniqueNameForFunction(name, index);
        }
    );
  }

  @VisibleForTesting
  SqlToJavaVisitor(
      LogicalSchema schema, FunctionRegistry functionRegistry,
      Function<ColumnRef, String> colRefToCodeName, Function<FunctionName, String> funNameToCodeName
  ) {
    this.expressionTypeManager =
        new ExpressionTypeManager(schema, functionRegistry);
    this.schema = Objects.requireNonNull(schema, "schema");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.colRefToCodeName = Objects.requireNonNull(colRefToCodeName, "colRefToCodeName");
    this.funNameToCodeName = Objects.requireNonNull(funNameToCodeName, "funNameToCodeName");
  }

  public String process(Expression expression) {
    return formatExpression(expression);
  }

  private String formatExpression(Expression expression) {
    Pair<String, SqlType> expressionFormatterResult =
        new Formatter(functionRegistry).process(expression, null);
    return expressionFormatterResult.getLeft();
  }


  private class Formatter implements ExpressionVisitor<Pair<String, SqlType>, Void> {

    private final FunctionRegistry functionRegistry;

    Formatter(FunctionRegistry functionRegistry) {
      this.functionRegistry = functionRegistry;
    }

    private Pair<String, SqlType> visitIllegalState(Expression expression) {
      throw new IllegalStateException(
          format("expression type %s should never be visited", expression.getClass()));
    }

    private Pair<String, SqlType> visitUnsupported(Expression expression) {
      throw new UnsupportedOperationException(
          format(
              "not yet implemented: %s.visit%s",
              getClass().getName(),
              expression.getClass().getSimpleName()
          )
      );
    }

    @Override
    public Pair<String, SqlType> visitType(Type node, Void context) {
      return visitIllegalState(node);
    }

    @Override
    public Pair<String, SqlType> visitWhenClause(WhenClause whenClause, Void context) {
      return visitIllegalState(whenClause);
    }

    @Override
    public Pair<String, SqlType> visitInPredicate(InPredicate inPredicate, Void context) {
      return visitUnsupported(inPredicate);
    }

    @Override
    public Pair<String, SqlType> visitInListExpression(
        InListExpression inListExpression, Void context
    ) {
      return visitUnsupported(inListExpression);
    }

    @Override
    public Pair<String, SqlType> visitTimestampLiteral(
        TimestampLiteral timestampLiteral, Void context
    ) {
      return visitUnsupported(timestampLiteral);
    }

    @Override
    public Pair<String, SqlType> visitTimeLiteral(TimeLiteral timeLiteral, Void context) {
      return visitUnsupported(timeLiteral);
    }

    @Override
    public Pair<String, SqlType> visitDecimalLiteral(DecimalLiteral decimalLiteral, Void context) {
      return visitUnsupported(decimalLiteral);
    }

    @Override
    public Pair<String, SqlType> visitSimpleCaseExpression(
        SimpleCaseExpression simpleCaseExpression, Void context
    ) {
      return visitUnsupported(simpleCaseExpression);
    }

    @Override
    public Pair<String, SqlType> visitBooleanLiteral(BooleanLiteral node, Void context) {
      return new Pair<>(String.valueOf(node.getValue()), SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<String, SqlType> visitStringLiteral(StringLiteral node, Void context) {
      return new Pair<>(
          "\"" + StringEscapeUtils.escapeJava(node.getValue()) + "\"",
          SqlTypes.STRING
      );
    }

    @Override
    public Pair<String, SqlType> visitDoubleLiteral(DoubleLiteral node, Void context) {
      return new Pair<>(Double.toString(node.getValue()), SqlTypes.DOUBLE);
    }

    @Override
    public Pair<String, SqlType> visitNullLiteral(NullLiteral node, Void context) {
      return new Pair<>("null", null);
    }

    @Override
    public Pair<String, SqlType> visitColumnReference(ColumnReferenceExp node, Void context) {
      ColumnRef fieldName = node.getReference();
      Column schemaColumn = schema.findValueColumn(node.getReference())
          .orElseThrow(() ->
              new KsqlException("Field not found: " + node.getReference()));

      return new Pair<>(colRefToCodeName.apply(fieldName), schemaColumn.type());
    }

    @Override
    public Pair<String, SqlType> visitDereferenceExpression(
        DereferenceExpression node, Void context
    ) {
      String instanceName = funNameToCodeName
          .apply(FetchFieldFromStruct.FUNCTION_NAME);

      SqlType functionReturnSchema = expressionTypeManager.getExpressionSqlType(node);
      String javaReturnType =
          SchemaConverters.sqlToJavaConverter().toJavaType(functionReturnSchema).getSimpleName();

      String arguments =
          process(node.getBase(), context).getLeft()
              + ", "
              + process(new StringLiteral(node.getFieldName()), context).getLeft();

      String codeString = "((" + javaReturnType + ") " + instanceName
          + ".evaluate(" + arguments + "))";

      return new Pair<>(codeString, functionReturnSchema);
    }

    public Pair<String, SqlType> visitLongLiteral(LongLiteral node, Void context) {
      return new Pair<>(node.getValue() + "L", SqlTypes.BIGINT);
    }

    @Override
    public Pair<String, SqlType> visitIntegerLiteral(IntegerLiteral node, Void context) {
      return new Pair<>(Integer.toString(node.getValue()), SqlTypes.INTEGER);
    }

    @Override
    public Pair<String, SqlType> visitFunctionCall(FunctionCall node, Void context) {
      FunctionName functionName = node.getName();

      String instanceName = funNameToCodeName.apply(functionName);

      SqlType functionReturnSchema = getFunctionReturnSchema(node);
      String javaReturnType =
          SchemaConverters.sqlToJavaConverter().toJavaType(functionReturnSchema).getSimpleName();
      String arguments = node.getArguments().stream()
          .map(arg -> process(arg, context).getLeft())
          .collect(Collectors.joining(", "));
      String codeString = "((" + javaReturnType + ") " + instanceName
          + ".evaluate(" + arguments + "))";
      return new Pair<>(codeString, functionReturnSchema);
    }

    private SqlType getFunctionReturnSchema(FunctionCall node) {
      UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName().name());
      List<SqlType> argumentSchemas = node.getArguments().stream()
          .map(expressionTypeManager::getExpressionSqlType)
          .collect(Collectors.toList());

      return udfFactory.getFunction(argumentSchemas).getReturnType(argumentSchemas);
    }

    @Override
    public Pair<String, SqlType> visitLogicalBinaryExpression(
        LogicalBinaryExpression node, Void context
    ) {
      if (node.getType() == LogicalBinaryExpression.Type.OR) {
        return new Pair<>(
            formatBinaryExpression(" || ", node.getLeft(), node.getRight(), context),
            SqlTypes.BOOLEAN
        );
      } else if (node.getType() == LogicalBinaryExpression.Type.AND) {
        return new Pair<>(
            formatBinaryExpression(" && ", node.getLeft(), node.getRight(), context),
            SqlTypes.BOOLEAN
        );
      }
      throw new UnsupportedOperationException(
          format("not yet implemented: %s.visit%s", getClass().getName(),
              node.getClass().getSimpleName()
          )
      );
    }

    @Override
    public Pair<String, SqlType> visitNotExpression(NotExpression node, Void context) {
      String exprString = process(node.getValue(), context).getLeft();
      return new Pair<>("(!" + exprString + ")", SqlTypes.BOOLEAN);
    }

    private String nullCheckPrefix(ComparisonExpression.Type type) {
      if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
        return "(((Object)(%1$s)) == null || ((Object)(%2$s)) == null) ? "
            + "((((Object)(%1$s)) == null ) ^ (((Object)(%2$s)) == null )) : ";
      }
      return "(((Object)(%1$s)) == null || ((Object)(%2$s)) == null) ? false : ";
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

    private String visitBytesComparisonExpression(
        ComparisonExpression.Type type, SqlType left, SqlType right
    ) {
      String comparator = SQL_COMPARE_TO_JAVA.get(type);
      if (comparator == null) {
        throw new KsqlException("Unexpected scalar comparison: " + type.getValue());
      }

      return String.format(
          "(%s.compareTo(%s) %s 0)",
          toDecimal(left, 1),
          toDecimal(right, 2),
          comparator
      );
    }

    private String toDecimal(SqlType schema, int index) {
      if (schema.baseType() == SqlBaseType.DECIMAL) {
        return "%" + index + "$s";
      }

      return "new BigDecimal(%" + index + "$s)";
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
    public Pair<String, SqlType> visitComparisonExpression(
        ComparisonExpression node, Void context
    ) {
      Pair<String, SqlType> left = process(node.getLeft(), context);
      Pair<String, SqlType> right = process(node.getRight(), context);

      String exprFormat = nullCheckPrefix(node.getType());

      if (left.getRight().baseType() == SqlBaseType.DECIMAL
          || right.getRight().baseType() == SqlBaseType.DECIMAL) {
        exprFormat += visitBytesComparisonExpression(
            node.getType(), left.getRight(), right.getRight());
      } else {
        switch (left.getRight().baseType()) {
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
      }
      String expr = "(" + String.format(exprFormat, left.getLeft(), right.getLeft()) + ")";
      return new Pair<>(expr, SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<String, SqlType> visitCast(Cast node, Void context) {
      Pair<String, SqlType> expr = process(node.getExpression(), context);
      return CastVisitor.getCast(expr, node.getType().getSqlType());
    }

    @Override
    public Pair<String, SqlType> visitIsNullPredicate(IsNullPredicate node, Void context) {
      Pair<String, SqlType> value = process(node.getValue(), context);
      return new Pair<>("((" + value.getLeft() + ") == null )", SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<String, SqlType> visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
      Pair<String, SqlType> value = process(node.getValue(), context);
      return new Pair<>("((" + value.getLeft() + ") != null )", SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<String, SqlType> visitArithmeticUnary(
        ArithmeticUnaryExpression node, Void context
    ) {
      Pair<String, SqlType> value = process(node.getValue(), context);
      switch (node.getSign()) {
        case MINUS:
          return visitArithmeticMinus(value);
        case PLUS:
          return visitArithmeticPlus(value);
        default:
          throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
      }
    }

    private Pair<String, SqlType> visitArithmeticMinus(Pair<String, SqlType> value) {
      if (value.getRight().baseType() == SqlBaseType.DECIMAL) {
        return new Pair<>(
            String.format(
                "(%s.negate(new MathContext(%d, RoundingMode.UNNECESSARY)))",
                value.getLeft(),
                ((SqlDecimal) value.getRight()).getPrecision()
            ),
            value.getRight()
        );
      } else {
        // this is to avoid turning a sequence of "-" into a comment (i.e., "-- comment")
        String separator = value.getLeft().startsWith("-") ? " " : "";
        return new Pair<>("-" + separator + value.getLeft(), value.getRight());
      }
    }

    private Pair<String, SqlType> visitArithmeticPlus(Pair<String, SqlType> value) {
      if (value.getRight().baseType() == SqlBaseType.DECIMAL) {
        return new Pair<>(
            String.format(
                "(%s.plus(new MathContext(%d, RoundingMode.UNNECESSARY)))",
                value.getLeft(),
                ((SqlDecimal) value.getRight()).getPrecision()
            ),
            value.getRight()
        );
      } else {
        return new Pair<>("+" + value.getLeft(), value.getRight());
      }
    }

    @Override
    public Pair<String, SqlType> visitArithmeticBinary(
        ArithmeticBinaryExpression node, Void context
    ) {
      Pair<String, SqlType> left = process(node.getLeft(), context);
      Pair<String, SqlType> right = process(node.getRight(), context);

      SqlType schema = expressionTypeManager.getExpressionSqlType(node);

      if (schema.baseType() == SqlBaseType.DECIMAL) {
        SqlDecimal decimal = (SqlDecimal) schema;
        String leftExpr =
            CastVisitor.getCast(left, DecimalUtil.toSqlDecimal(left.right)).getLeft();
        String rightExpr =
            CastVisitor.getCast(right, DecimalUtil.toSqlDecimal(right.right)).getLeft();

        return new Pair<>(
            String.format(
                "(%s.%s(%s, new MathContext(%d, RoundingMode.UNNECESSARY)).setScale(%d))",
                leftExpr,
                DECIMAL_OPERATOR_NAME.get(node.getOperator()),
                rightExpr,
                decimal.getPrecision(),
                decimal.getScale()
            ),
            schema
        );
      } else {
        String leftExpr =
            left.getRight().baseType() == SqlBaseType.DECIMAL
                ? CastVisitor.getCast(left, SqlTypes.DOUBLE).getLeft()
                : left.getLeft();
        String rightExpr =
            right.getRight().baseType() == SqlBaseType.DECIMAL
                ? CastVisitor.getCast(right, SqlTypes.DOUBLE).getLeft()
                : right.getLeft();

        return new Pair<>(
            String.format(
                "(%s %s %s)",
                leftExpr,
                node.getOperator().getSymbol(),
                rightExpr
            ),
            schema
        );
      }
    }

    @Override
    public Pair<String, SqlType> visitSearchedCaseExpression(
        SearchedCaseExpression node, Void context
    ) {
      String functionClassName = SearchedCaseFunction.class.getSimpleName();
      List<CaseWhenProcessed> whenClauses = node
          .getWhenClauses()
          .stream()
          .map(whenClause -> new CaseWhenProcessed(
              process(whenClause.getOperand(), context),
              process(whenClause.getResult(), context)
          ))
          .collect(Collectors.toList());

      SqlType resultSchema = expressionTypeManager.getExpressionSqlType(node);
      String resultSchemaString =
          SchemaConverters.sqlToJavaConverter().toJavaType(resultSchema).getCanonicalName();

      List<String> lazyWhenClause = whenClauses
          .stream()
          .map(processedWhenClause -> functionClassName + ".whenClause("
              + buildSupplierCode(
              "Boolean", processedWhenClause.whenProcessResult.getLeft())
              + ", "
              + buildSupplierCode(
              resultSchemaString, processedWhenClause.thenProcessResult.getLeft())
              + ")")
          .collect(Collectors.toList());

      String defaultValue = node.getDefaultValue().isPresent()
          ? process(node.getDefaultValue().get(), context).getLeft()
          : "null";

      String codeString = "((" + resultSchemaString + ")"
          + functionClassName + ".searchedCaseFunction(ImmutableList.of( "
          + StringUtils.join(lazyWhenClause, ", ") + "),"
          + buildSupplierCode(resultSchemaString, defaultValue)
          + "))";
      return new Pair<>(codeString, resultSchema);
    }

    private String buildSupplierCode(String typeString, String code) {
      return " new " + Supplier.class.getSimpleName() + "<" + typeString + ">() {"
          + " @Override public " + typeString + " get() { return " + code + "; }}";
    }

    @Override
    public Pair<String, SqlType> visitLikePredicate(LikePredicate node, Void context) {

      // For now we just support simple prefix/suffix cases only.
      String patternString = trimQuotes(process(node.getPattern(), context).getLeft());
      String valueString = process(node.getValue(), context).getLeft();
      if (patternString.startsWith("%")) {
        if (patternString.endsWith("%")) {
          return new Pair<>(
              "(" + valueString + ").contains(\""
                  + patternString.substring(1, patternString.length() - 1)
                  + "\")",
              SqlTypes.STRING
          );
        } else {
          return new Pair<>(
              "(" + valueString + ").endsWith(\"" + patternString.substring(1) + "\")",
              SqlTypes.STRING
          );
        }
      }

      if (patternString.endsWith("%")) {
        return new Pair<>(
            "(" + valueString + ")"
                + ".startsWith(\""
                + patternString.substring(0, patternString.length() - 1) + "\")",
            SqlTypes.STRING
        );
      }

      if (!patternString.contains("%")) {
        return new Pair<>(
            "(" + valueString + ")"
                + ".equals(\""
                + patternString + "\")",
            SqlTypes.STRING
        );
      }

      throw new UnsupportedOperationException(
          "KSQL only supports leading and trailing wildcards in LIKE expressions."
      );
    }

    @Override
    public Pair<String, SqlType> visitSubscriptExpression(SubscriptExpression node, Void context) {
      SqlType internalSchema = expressionTypeManager.getExpressionSqlType(node.getBase());

      String internalSchemaJavaType =
          SchemaConverters.sqlToJavaConverter().toJavaType(internalSchema).getCanonicalName();
      switch (internalSchema.baseType()) {
        case ARRAY:
          SqlArray array = (SqlArray) internalSchema;
          String listName = process(node.getBase(), context).getLeft();
          String suppliedIdx = process(node.getIndex(), context).getLeft();
          String trueIdx = node.getIndex().toString().startsWith("-")
              ? String.format("((%s)%s).size()%s", internalSchemaJavaType, listName, suppliedIdx)
              : suppliedIdx;

          String code = format(
              "((%s) ((%s)%s).get((int)%s))",
              SchemaConverters.sqlToJavaConverter().toJavaType(array.getItemType()).getSimpleName(),
              internalSchemaJavaType,
              listName,
              trueIdx
          );

          return new Pair<>(code, array.getItemType());

        case MAP:
          SqlMap map = (SqlMap) internalSchema;
          return new Pair<>(
              String.format(
                  "((%s) ((%s)%s).get(%s))",
                  SchemaConverters.sqlToJavaConverter()
                      .toJavaType(map.getValueType()).getSimpleName(),
                  internalSchemaJavaType,
                  process(node.getBase(), context).getLeft(),
                  process(node.getIndex(), context).getLeft()
              ),
              map.getValueType()
          );
        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public Pair<String, SqlType> visitBetweenPredicate(BetweenPredicate node, Void context) {
      Pair<String, SqlType> value = process(node.getValue(), context);
      Pair<String, SqlType> min = process(node.getMin(), context);
      Pair<String, SqlType> max = process(node.getMax(), context);

      String expression = "(((Object) {value}) == null "
          + "|| ((Object) {min}) == null "
          + "|| ((Object) {max}) == null) "
          + "? false "
          + ": ";

      SqlBaseType type = value.getRight().baseType();
      switch (type) {
        case DOUBLE:
        case BIGINT:
        case INTEGER:
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
      String evaluation = StrSubstitutor.replace(
          "(" + expression + ")",
          ImmutableMap.of(
              "value", value.getLeft(),
              "min", min.getLeft(),
              "max", max.getLeft()
          ),
          "{", "}"
      );

      return new Pair<>(evaluation, SqlTypes.BOOLEAN);
    }

    private String formatBinaryExpression(
        String operator, Expression left, Expression right, Void context
    ) {
      return "(" + process(left, context).getLeft() + " " + operator + " "
          + process(right, context).getLeft() + ")";
    }

    private String trimQuotes(String s) {
      return s.substring(1, s.length() - 1);
    }
  }

  private static final class CastVisitor {

    private static final Map<SqlBaseType, CastVisitor.CastFunction> CASTERS = ImmutableMap
        .<SqlBaseType, CastVisitor.CastFunction>builder()
        .put(SqlBaseType.STRING, CastVisitor::castString)
        .put(SqlBaseType.BOOLEAN, CastVisitor::castBoolean)
        .put(SqlBaseType.INTEGER, CastVisitor::castInteger)
        .put(SqlBaseType.BIGINT, CastVisitor::castLong)
        .put(SqlBaseType.DOUBLE, CastVisitor::castDouble)
        .put(SqlBaseType.DECIMAL, CastVisitor::castDecimal)
        .build();

    private CastVisitor() {
    }

    static Pair<String, SqlType> getCast(Pair<String, SqlType> expr, SqlType sqlType) {
      if (!sqlType.supportsCast()) {
        throw new KsqlFunctionException(
            "Only casts to primitive types and decimal are supported: " + sqlType);
      }

      SqlType rightSchema = expr.getRight();
      if (sqlType.equals(rightSchema) || rightSchema == null) {
        return new Pair<>(expr.getLeft(), sqlType);
      }

      return CASTERS.getOrDefault(
          sqlType.baseType(),
          (e, t, r) -> {
            throw new KsqlException("Invalid cast operation: " + t);
          }
      )
          .cast(expr, sqlType, sqlType);
    }

    private static Pair<String, SqlType> castString(
        Pair<String, SqlType> expr, SqlType sqltype, SqlType returnType
    ) {
      SqlType schema = expr.getRight();
      String exprStr;
      if (schema.baseType() == SqlBaseType.DECIMAL) {
        SqlDecimal decimal = (SqlDecimal) schema;
        int precision = decimal.getPrecision();
        int scale = decimal.getScale();
        exprStr = String.format("DecimalUtil.format(%d, %d, %s)", precision, scale, expr.getLeft());
      } else {
        exprStr = "String.valueOf(" + expr.getLeft() + ")";
      }
      return new Pair<>(exprStr, returnType);
    }

    private static Pair<String, SqlType> castBoolean(
        Pair<String, SqlType> expr, SqlType sqltype, SqlType returnType
    ) {
      return new Pair<>(getCastToBooleanString(expr.getRight(), expr.getLeft()), returnType);
    }

    private static Pair<String, SqlType> castInteger(
        Pair<String, SqlType> expr, SqlType sqltype, SqlType returnType
    ) {
      String exprStr = getCastString(
          expr.getRight(),
          expr.getLeft(),
          "intValue()",
          "Integer.parseInt"
      );
      return new Pair<>(exprStr, returnType);
    }

    private static Pair<String, SqlType> castLong(
        Pair<String, SqlType> expr, SqlType sqltype, SqlType returnType
    ) {
      String exprStr = getCastString(
          expr.getRight(),
          expr.getLeft(),
          "longValue()",
          "Long.parseLong"
      );
      return new Pair<>(exprStr, returnType);
    }

    private static Pair<String, SqlType> castDouble(
        Pair<String, SqlType> expr, SqlType sqltype, SqlType returnType
    ) {
      String exprStr = getCastString(
          expr.getRight(),
          expr.getLeft(),
          "doubleValue()",
          "Double.parseDouble"
      );
      return new Pair<>(exprStr, returnType);
    }

    private static Pair<String, SqlType> castDecimal(
        Pair<String, SqlType> expr, SqlType sqltype, SqlType returnType
    ) {
      if (!(sqltype instanceof SqlDecimal)) {
        throw new KsqlException("Expected decimal type: " + sqltype);
      }

      SqlDecimal sqlDecimal = (SqlDecimal) sqltype;

      if (expr.getRight().baseType() == SqlBaseType.DECIMAL && expr.right.equals(sqlDecimal)) {
        return expr;
      }

      return new Pair<>(
          getDecimalCastString(expr.getRight(), expr.getLeft(), sqlDecimal),
          returnType
      );
    }

    private static String getCastToBooleanString(SqlType schema, String exprStr) {
      if (schema.baseType() == SqlBaseType.STRING) {
        return "Boolean.parseBoolean(" + exprStr + ")";
      } else {
        throw new KsqlFunctionException(
            "Invalid cast operation: Cannot cast " + exprStr + " to boolean.");
      }
    }

    private static String getCastString(
        SqlType schema, String exprStr, String javaTypeMethod, String javaStringParserMethod
    ) {
      if (schema.baseType() == SqlBaseType.DECIMAL) {
        return "((" + exprStr + ")." + javaTypeMethod + ")";
      }

      switch (schema.baseType()) {
        case INTEGER:
          return "(new Integer(" + exprStr + ")." + javaTypeMethod + ")";
        case BIGINT:
          return "(new Long(" + exprStr + ")." + javaTypeMethod + ")";
        case DOUBLE:
          return "(new Double(" + exprStr + ")." + javaTypeMethod + ")";
        case STRING:
          return javaStringParserMethod + "(" + exprStr + ")";

        default:
          throw new KsqlFunctionException(
              "Invalid cast operation: Cannot cast "
                  + exprStr + " to " + schema.toString(FormatOptions.noEscape()) + "."
          );
      }
    }

    private static String getDecimalCastString(SqlType schema, String exprStr, SqlDecimal target) {

      switch (schema.baseType()) {
        case INTEGER:
        case BIGINT:
        case DOUBLE:
        case STRING:
        case DECIMAL:
          return String.format(
              "(DecimalUtil.cast(%s, %d, %d))",
              exprStr,
              target.getPrecision(),
              target.getScale()
          );
        default:
          throw new KsqlFunctionException(
              "Invalid cast operation: Cannot cast " + exprStr + " to " + schema);
      }
    }

    @FunctionalInterface
    private interface CastFunction {

      Pair<String, SqlType> cast(
          Pair<String, SqlType> expr,
          SqlType sqltype,
          SqlType returnType
      );
    }
  }

  private static final class CaseWhenProcessed {

    private final Pair<String, SqlType> whenProcessResult;
    private final Pair<String, SqlType> thenProcessResult;

    private CaseWhenProcessed(
        Pair<String, SqlType> whenProcessResult, Pair<String, SqlType> thenProcessResult
    ) {
      this.whenProcessResult = whenProcessResult;
      this.thenProcessResult = thenProcessResult;
    }
  }

}
