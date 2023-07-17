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

import static io.confluent.ksql.schema.ksql.SchemaConverters.sqlToFunctionConverter;
import static java.lang.String.format;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultiset;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import io.confluent.ksql.execution.codegen.helpers.ArrayAccess;
import io.confluent.ksql.execution.codegen.helpers.ArrayBuilder;
import io.confluent.ksql.execution.codegen.helpers.LikeEvaluator;
import io.confluent.ksql.execution.codegen.helpers.MapBuilder;
import io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression.Field;
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
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.schema.utils.FormatOptions;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class SqlToJavaVisitor {

  public static final List<String> JAVA_IMPORTS = ImmutableList.of(
      "org.apache.kafka.connect.data.Struct",
      "io.confluent.ksql.execution.codegen.helpers.ArrayAccess",
      "io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction",
      "io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction.LazyWhenClause",
      "java.util.HashMap",
      "java.util.Map",
      "java.util.List",
      "java.util.ArrayList",
      "com.google.common.collect.ImmutableList",
      "com.google.common.collect.ImmutableMap",
      "java.util.function.Supplier",
      DecimalUtil.class.getCanonicalName(),
      BigDecimal.class.getCanonicalName(),
      MathContext.class.getCanonicalName(),
      RoundingMode.class.getCanonicalName(),
      SchemaBuilder.class.getCanonicalName(),
      Struct.class.getCanonicalName(),
      ArrayBuilder.class.getCanonicalName(),
      LikeEvaluator.class.getCanonicalName(),
      MapBuilder.class.getCanonicalName()
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
  private final Function<ColumnName, String> colRefToCodeName;
  private final Function<CreateStructExpression, String> structToCodeName;

  public static SqlToJavaVisitor of(
      final LogicalSchema schema, final FunctionRegistry functionRegistry, final CodeGenSpec spec
  ) {
    final Multiset<FunctionName> nameCounts = HashMultiset.create();
    return new SqlToJavaVisitor(
        schema,
        functionRegistry,
        spec::getCodeName,
        name -> {
          final int index = nameCounts.add(name, 1);
          return spec.getUniqueNameForFunction(name, index);
        },
        spec::getStructSchemaName);
  }

  @VisibleForTesting
  SqlToJavaVisitor(
      final LogicalSchema schema, final FunctionRegistry functionRegistry,
      final Function<ColumnName, String> colRefToCodeName,
      final Function<FunctionName, String> funNameToCodeName,
      final Function<CreateStructExpression, String> structToCodeName
  ) {
    this.expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);
    this.schema = Objects.requireNonNull(schema, "schema");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.colRefToCodeName = Objects.requireNonNull(colRefToCodeName, "colRefToCodeName");
    this.funNameToCodeName = Objects.requireNonNull(funNameToCodeName, "funNameToCodeName");
    this.structToCodeName = Objects.requireNonNull(structToCodeName, "structToCodeName");
  }

  public String process(final Expression expression) {
    return formatExpression(expression);
  }

  private String formatExpression(final Expression expression) {
    final Pair<String, SqlType> expressionFormatterResult =
        new Formatter(functionRegistry).process(expression, null);
    return expressionFormatterResult.getLeft();
  }


  private class Formatter implements ExpressionVisitor<Pair<String, SqlType>, Void> {

    private final FunctionRegistry functionRegistry;

    Formatter(final FunctionRegistry functionRegistry) {
      this.functionRegistry = functionRegistry;
    }

    private Pair<String, SqlType> visitIllegalState(final Expression expression) {
      throw new IllegalStateException(
          format("expression type %s should never be visited", expression.getClass()));
    }

    private Pair<String, SqlType> visitUnsupported(final Expression expression) {
      throw new UnsupportedOperationException(
          format(
              "not yet implemented: %s.visit%s",
              getClass().getName(),
              expression.getClass().getSimpleName()
          )
      );
    }

    @Override
    public Pair<String, SqlType> visitType(final Type node, final Void context) {
      return visitIllegalState(node);
    }

    @Override
    public Pair<String, SqlType> visitWhenClause(final WhenClause whenClause, final Void context) {
      return visitIllegalState(whenClause);
    }

    @Override
    public Pair<String, SqlType> visitInPredicate(
        final InPredicate inPredicate,
        final Void context
    ) {
      return visitUnsupported(inPredicate);
    }

    @Override
    public Pair<String, SqlType> visitInListExpression(
        final InListExpression inListExpression, final Void context
    ) {
      return visitUnsupported(inListExpression);
    }

    @Override
    public Pair<String, SqlType> visitTimestampLiteral(
        final TimestampLiteral timestampLiteral, final Void context
    ) {
      return visitUnsupported(timestampLiteral);
    }

    @Override
    public Pair<String, SqlType> visitTimeLiteral(
        final TimeLiteral timeLiteral,
        final Void context
    ) {
      return visitUnsupported(timeLiteral);
    }

    @Override
    public Pair<String, SqlType> visitSimpleCaseExpression(
        final SimpleCaseExpression simpleCaseExpression, final Void context
    ) {
      return visitUnsupported(simpleCaseExpression);
    }

    @Override
    public Pair<String, SqlType> visitBooleanLiteral(
        final BooleanLiteral node,
        final Void context
    ) {
      return new Pair<>(String.valueOf(node.getValue()), SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<String, SqlType> visitStringLiteral(final StringLiteral node, final Void context) {
      return new Pair<>(
          "\"" + StringEscapeUtils.escapeJava(node.getValue()) + "\"",
          SqlTypes.STRING
      );
    }

    @Override
    public Pair<String, SqlType> visitDoubleLiteral(final DoubleLiteral node, final Void context) {
      return new Pair<>(node.toString(), SqlTypes.DOUBLE);
    }

    @Override
    public Pair<String, SqlType> visitDecimalLiteral(
        final DecimalLiteral decimalLiteral,
        final Void context
    ) {
      return new Pair<>(
          "new BigDecimal(\"" + decimalLiteral.getValue() + "\")",
          DecimalUtil.fromValue(decimalLiteral.getValue())
      );
    }

    @Override
    public Pair<String, SqlType> visitNullLiteral(final NullLiteral node, final Void context) {
      return new Pair<>("null", null);
    }

    @Override
    public Pair<String, SqlType> visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      final ColumnName fieldName = node.getColumnName();
      final Column schemaColumn = schema.findValueColumn(node.getColumnName())
          .orElseThrow(() ->
              new KsqlException("Field not found: " + node.getColumnName()));

      return new Pair<>(colRefToCodeName.apply(fieldName), schemaColumn.type());
    }

    @Override
    public Pair<String, SqlType> visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Void context
    ) {
      throw new UnsupportedOperationException(
          "Qualified column reference must be resolved to unqualified reference before codegen"
      );
    }

    @Override
    public Pair<String, SqlType> visitDereferenceExpression(
        final DereferenceExpression node, final Void context
    ) {
      final SqlType functionReturnSchema = expressionTypeManager.getExpressionSqlType(node);
      final String javaReturnType =
          SchemaConverters.sqlToJavaConverter().toJavaType(functionReturnSchema).getSimpleName();

      final String struct = process(node.getBase(), context).getLeft();
      final String field = process(new StringLiteral(node.getFieldName()), context).getLeft();
      final String codeString = "((" + javaReturnType + ") "
          + struct + ".get(" + field + "))";

      return new Pair<>(codeString, functionReturnSchema);
    }

    public Pair<String, SqlType> visitLongLiteral(final LongLiteral node, final Void context) {
      return new Pair<>(node.getValue() + "L", SqlTypes.BIGINT);
    }

    @Override
    public Pair<String, SqlType> visitIntegerLiteral(
        final IntegerLiteral node,
        final Void context
    ) {
      return new Pair<>(Integer.toString(node.getValue()), SqlTypes.INTEGER);
    }

    @Override
    public Pair<String, SqlType> visitFunctionCall(final FunctionCall node, final Void context) {
      final FunctionName functionName = node.getName();

      final String instanceName = funNameToCodeName.apply(functionName);

      final UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName());
      final List<SqlType> argumentSchemas = node.getArguments().stream()
          .map(expressionTypeManager::getExpressionSqlType)
          .collect(Collectors.toList());

      final KsqlFunction function = udfFactory.getFunction(argumentSchemas);

      final SqlType functionReturnSchema = function.getReturnType(argumentSchemas);
      final String javaReturnType =
          SchemaConverters.sqlToJavaConverter().toJavaType(functionReturnSchema).getSimpleName();

      final List<Expression> arguments = node.getArguments();

      final StringJoiner joiner = new StringJoiner(", ");
      for (int i = 0; i < arguments.size(); i++) {
        final Expression arg = arguments.get(i);
        final SqlType sqlType = argumentSchemas.get(i);

        final ParamType paramType;
        if (i >= function.parameters().size() - 1 && function.isVariadic()) {
          paramType = ((ArrayType) Iterables.getLast(function.parameters())).element();
        } else {
          paramType = function.parameters().get(i);
        }

        joiner.add(process(convertArgument(arg, sqlType, paramType), context).getLeft());
      }


      final String argumentsString = joiner.toString();
      final String codeString = "((" + javaReturnType + ") " + instanceName
          + ".evaluate(" + argumentsString + "))";
      return new Pair<>(codeString, functionReturnSchema);
    }

    private Expression convertArgument(
        final Expression argument,
        final SqlType argType,
        final ParamType funType
    ) {
      if (argType == null
          || GenericsUtil.hasGenerics(funType)
          || sqlToFunctionConverter().toFunctionType(argType).equals(funType)) {
        return argument;
      }

      final SqlType target = funType == ParamTypes.DECIMAL
          ? DecimalUtil.toSqlDecimal(argType)
          : SchemaConverters.functionToSqlConverter().toSqlType(funType);
      return new Cast(argument, new Type(target));
    }

    @Override
    public Pair<String, SqlType> visitLogicalBinaryExpression(
        final LogicalBinaryExpression node, final Void context
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
    public Pair<String, SqlType> visitNotExpression(final NotExpression node, final Void context) {
      final String exprString = process(node.getValue(), context).getLeft();
      return new Pair<>("(!" + exprString + ")", SqlTypes.BOOLEAN);
    }

    private String nullCheckPrefix(final ComparisonExpression.Type type) {
      if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
        return "(((Object)(%1$s)) == null || ((Object)(%2$s)) == null) ? "
            + "((((Object)(%1$s)) == null ) ^ (((Object)(%2$s)) == null )) : ";
      }
      return "(((Object)(%1$s)) == null || ((Object)(%2$s)) == null) ? false : ";
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

    private String visitBytesComparisonExpression(
        final ComparisonExpression.Type type, final SqlType left, final SqlType right
    ) {
      final String comparator = SQL_COMPARE_TO_JAVA.get(type);
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

    private String toDecimal(final SqlType schema, final int index) {
      switch (schema.baseType()) {
        case DECIMAL:
          return "%" + index + "$s";
        case DOUBLE:
          return "BigDecimal.valueOf(%" + index + "$s)";
        default:
          return "new BigDecimal(%" + index + "$s)";
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
    public Pair<String, SqlType> visitComparisonExpression(
        final ComparisonExpression node, final Void context
    ) {
      final Pair<String, SqlType> left = process(node.getLeft(), context);
      final Pair<String, SqlType> right = process(node.getRight(), context);

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
      final String expr = "(" + String.format(exprFormat, left.getLeft(), right.getLeft()) + ")";
      return new Pair<>(expr, SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<String, SqlType> visitCast(final Cast node, final Void context) {
      final Pair<String, SqlType> expr = process(node.getExpression(), context);
      return CastVisitor.getCast(expr, node.getType().getSqlType());
    }

    @Override
    public Pair<String, SqlType> visitIsNullPredicate(
        final IsNullPredicate node,
        final Void context
    ) {
      final Pair<String, SqlType> value = process(node.getValue(), context);
      return new Pair<>("((" + value.getLeft() + ") == null )", SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<String, SqlType> visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final Void context
    ) {
      final Pair<String, SqlType> value = process(node.getValue(), context);
      return new Pair<>("((" + value.getLeft() + ") != null )", SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<String, SqlType> visitArithmeticUnary(
        final ArithmeticUnaryExpression node, final Void context
    ) {
      final Pair<String, SqlType> value = process(node.getValue(), context);
      switch (node.getSign()) {
        case MINUS:
          return visitArithmeticMinus(value);
        case PLUS:
          return visitArithmeticPlus(value);
        default:
          throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
      }
    }

    private Pair<String, SqlType> visitArithmeticMinus(final Pair<String, SqlType> value) {
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
        final String separator = value.getLeft().startsWith("-") ? " " : "";
        return new Pair<>("-" + separator + value.getLeft(), value.getRight());
      }
    }

    private Pair<String, SqlType> visitArithmeticPlus(final Pair<String, SqlType> value) {
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
        final ArithmeticBinaryExpression node, final Void context
    ) {
      final Pair<String, SqlType> left = process(node.getLeft(), context);
      final Pair<String, SqlType> right = process(node.getRight(), context);

      final SqlType schema = expressionTypeManager.getExpressionSqlType(node);

      if (schema.baseType() == SqlBaseType.DECIMAL) {
        final SqlDecimal decimal = (SqlDecimal) schema;
        final String leftExpr =
            CastVisitor.getCast(left, DecimalUtil.toSqlDecimal(left.right)).getLeft();
        final String rightExpr =
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
        final String leftExpr =
            left.getRight().baseType() == SqlBaseType.DECIMAL
                ? CastVisitor.getCast(left, SqlTypes.DOUBLE).getLeft()
                : left.getLeft();
        final String rightExpr =
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
        final SearchedCaseExpression node, final Void context
    ) {
      final String functionClassName = SearchedCaseFunction.class.getSimpleName();
      final List<CaseWhenProcessed> whenClauses = node
          .getWhenClauses()
          .stream()
          .map(whenClause -> new CaseWhenProcessed(
              process(whenClause.getOperand(), context),
              process(whenClause.getResult(), context)
          ))
          .collect(Collectors.toList());

      final SqlType resultSchema = expressionTypeManager.getExpressionSqlType(node);
      final String resultSchemaString =
          SchemaConverters.sqlToJavaConverter().toJavaType(resultSchema).getCanonicalName();

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
    public Pair<String, SqlType> visitLikePredicate(final LikePredicate node, final Void context) {

      final String patternString = process(node.getPattern(), context).getLeft();
      final String valueString = process(node.getValue(), context).getLeft();

      if (node.getEscape().isPresent()) {
        return new Pair<>(
            "LikeEvaluator.matches("
                + valueString + ", "
                + patternString + ", '"
                + node.getEscape().get() + "')",
            SqlTypes.STRING
        );
      } else {
        return new Pair<>(
            "LikeEvaluator.matches(" + valueString + ", " + patternString + ")",
            SqlTypes.STRING
        );
      }
    }

    @Override
    public Pair<String, SqlType> visitSubscriptExpression(
        final SubscriptExpression node,
        final Void context
    ) {
      final SqlType internalSchema = expressionTypeManager.getExpressionSqlType(node.getBase());

      final String internalSchemaJavaType =
          SchemaConverters.sqlToJavaConverter().toJavaType(internalSchema).getCanonicalName();
      switch (internalSchema.baseType()) {
        case ARRAY:
          final SqlArray array = (SqlArray) internalSchema;
          final String listName = process(node.getBase(), context).getLeft();
          final String suppliedIdx = process(node.getIndex(), context).getLeft();

          final String code = format(
              "((%s) (%s.arrayAccess((%s) %s, ((int) %s))))",
              SchemaConverters.sqlToJavaConverter().toJavaType(array.getItemType()).getSimpleName(),
              ArrayAccess.class.getSimpleName(),
              internalSchemaJavaType,
              listName,
              suppliedIdx
          );

          return new Pair<>(code, array.getItemType());

        case MAP:
          final SqlMap map = (SqlMap) internalSchema;
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
    public Pair<String, SqlType> visitCreateArrayExpression(
        final CreateArrayExpression exp,
        final Void context
    ) {
      final StringBuilder array = new StringBuilder("new ArrayBuilder(");
      array.append(exp.getValues().size());
      array.append((')'));

      for (Expression value : exp.getValues()) {
        array.append(".add(");
        array.append(process(value, context).getLeft());
        array.append(")");
      }
      return new Pair<>(
          "((List)" + array.toString() + ".build())",
          expressionTypeManager.getExpressionSqlType(exp));
    }

    @Override
    public Pair<String, SqlType> visitCreateMapExpression(
        final CreateMapExpression exp,
        final Void context
    ) {
      final StringBuilder map = new StringBuilder("new MapBuilder(");
      map.append(exp.getMap().size());
      map.append((')'));

      for (Entry<Expression, Expression> entry: exp.getMap().entrySet()) {
        map.append(".put(");
        map.append(process(entry.getKey(), context).getLeft());
        map.append(", ");
        map.append(process(entry.getValue(), context).getLeft());
        map.append(")");
      }

      return new Pair<>(
          "((Map)" + map.toString() + ".build())",
          expressionTypeManager.getExpressionSqlType(exp));
    }

    @Override
    public Pair<String, SqlType> visitStructExpression(
        final CreateStructExpression node,
        final Void context
    ) {
      final String schemaName = structToCodeName.apply(node);
      final StringBuilder struct = new StringBuilder("new Struct(").append(schemaName).append(")");
      for (final Field field : node.getFields()) {
        struct.append(".put(")
            .append('"')
            .append(field.getName())
            .append('"')
            .append(",")
            .append(process(field.getValue(), context).getLeft())
            .append(")");
      }
      return new Pair<>(
          "((Struct)" + struct.toString() + ")",
          expressionTypeManager.getExpressionSqlType(node)
      );
    }

    @Override
    public Pair<String, SqlType> visitBetweenPredicate(
        final BetweenPredicate node,
        final Void context
    ) {
      final Pair<String, SqlType> compareMin = process(
          new ComparisonExpression(
              ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
              node.getValue(),
              node.getMin()),
          context);
      final Pair<String, SqlType> compareMax = process(
          new ComparisonExpression(
              ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
              node.getValue(),
              node.getMax()),
          context);

      // note that the entire expression must be surrounded by parentheses
      // otherwise negations and other higher level operations will not work
      return new Pair<>(
          "(" + compareMin.getLeft() + " && " + compareMax.getLeft() + ")",
          SqlTypes.BOOLEAN
      );
    }

    private String formatBinaryExpression(
        final String operator, final Expression left, final Expression right, final Void context
    ) {
      return "(" + process(left, context).getLeft() + " " + operator + " "
          + process(right, context).getLeft() + ")";
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

    static Pair<String, SqlType> getCast(final Pair<String, SqlType> expr, final SqlType sqlType) {
      final SqlType sourceType = expr.getRight();
      if (sourceType == null || sqlType.equals(sourceType)) {
        // sourceType is null if source is SQL NULL
        return new Pair<>(expr.getLeft(), sqlType);
      }

      return Optional.ofNullable(
          CASTERS.get(sqlType.baseType())
          ).orElseThrow(
              () -> new KsqlFunctionException("Cast of " + expr.getRight()
                  + " to " + sqlType + " is not supported"))
          .cast(expr, sqlType);
    }

    private static Pair<String, SqlType> castString(
        final Pair<String, SqlType> expr, final SqlType returnType
    ) {
      final SqlType schema = expr.getRight();
      final String exprStr;
      if (schema.baseType() == SqlBaseType.DECIMAL) {
        final SqlDecimal decimal = (SqlDecimal) schema;
        final int precision = decimal.getPrecision();
        final int scale = decimal.getScale();
        exprStr = String.format("DecimalUtil.format(%d, %d, %s)", precision, scale, expr.getLeft());
      } else {
        exprStr = "String.valueOf(" + expr.getLeft() + ")";
      }
      return new Pair<>(exprStr, returnType);
    }

    private static Pair<String, SqlType> castBoolean(
        final Pair<String, SqlType> expr, final SqlType returnType
    ) {
      return new Pair<>(getCastToBooleanString(expr.getRight(), expr.getLeft()), returnType);
    }

    private static Pair<String, SqlType> castInteger(
        final Pair<String, SqlType> expr, final SqlType returnType
    ) {
      final String exprStr = getCastString(
          expr.getRight(),
          expr.getLeft(),
          "intValue()",
          "Integer.parseInt"
      );
      return new Pair<>(exprStr, returnType);
    }

    private static Pair<String, SqlType> castLong(
        final Pair<String, SqlType> expr, final SqlType returnType
    ) {
      final String exprStr = getCastString(
          expr.getRight(),
          expr.getLeft(),
          "longValue()",
          "Long.parseLong"
      );
      return new Pair<>(exprStr, returnType);
    }

    private static Pair<String, SqlType> castDouble(
        final Pair<String, SqlType> expr, final SqlType returnType
    ) {
      final String exprStr = getCastString(
          expr.getRight(),
          expr.getLeft(),
          "doubleValue()",
          "Double.parseDouble"
      );
      return new Pair<>(exprStr, returnType);
    }

    private static Pair<String, SqlType> castDecimal(
        final Pair<String, SqlType> expr, final SqlType returnType
    ) {
      if (!(returnType instanceof SqlDecimal)) {
        throw new KsqlException("Expected decimal type: " + returnType);
      }

      final SqlDecimal sqlDecimal = (SqlDecimal) returnType;

      if (expr.getRight().baseType() == SqlBaseType.DECIMAL && expr.right.equals(sqlDecimal)) {
        return expr;
      }

      return new Pair<>(
          getDecimalCastString(expr.getRight(), expr.getLeft(), sqlDecimal),
          returnType
      );
    }

    private static String getCastToBooleanString(final SqlType schema, final String exprStr) {
      if (schema.baseType() == SqlBaseType.STRING) {
        return "Boolean.parseBoolean(" + exprStr + ")";
      } else {
        throw new KsqlFunctionException(
            "Invalid cast operation: Cannot cast " + exprStr + " to boolean.");
      }
    }

    private static String getCastString(
        final SqlType schema,
        final String exprStr,
        final String javaTypeMethod,
        final String javaStringParserMethod
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

    private static String getDecimalCastString(
        final SqlType schema,
        final String exprStr,
        final SqlDecimal target
    ) {
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
          SqlType returnType
      );
    }
  }

  private static final class CaseWhenProcessed {

    private final Pair<String, SqlType> whenProcessResult;
    private final Pair<String, SqlType> thenProcessResult;

    private CaseWhenProcessed(
        final Pair<String, SqlType> whenProcessResult, final Pair<String, SqlType> thenProcessResult
    ) {
      this.whenProcessResult = whenProcessResult;
      this.thenProcessResult = thenProcessResult;
    }
  }
}
