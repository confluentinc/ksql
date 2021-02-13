/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.evaluator;

import static io.confluent.ksql.execution.evaluator.ComparisonInterpreter.doComparisonCheck;
import static io.confluent.ksql.execution.evaluator.ComparisonInterpreter.doEqualsCheck;
import static io.confluent.ksql.execution.evaluator.ComparisonInterpreter.doNumericalCompareTo;
import static io.confluent.ksql.schema.ksql.SchemaConverters.sqlToFunctionConverter;
import static java.lang.String.format;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.helpers.ArrayAccess;
import io.confluent.ksql.execution.codegen.helpers.ArrayBuilder;
import io.confluent.ksql.execution.codegen.helpers.LikeEvaluator;
import io.confluent.ksql.execution.codegen.helpers.MapBuilder;
import io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction;
import io.confluent.ksql.execution.codegen.helpers.SearchedCaseFunction.LazyWhenClause;
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
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.execution.util.CoercionUtil;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.GenericsUtil;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.types.ArrayType;
import io.confluent.ksql.function.types.ParamType;
import io.confluent.ksql.function.types.ParamTypes;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.logging.processing.RecordProcessingError;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class ExpressionInterpreter implements ExpressionEvaluator {

  private final ExpressionTypeManager expressionTypeManager;
  private final FunctionRegistry functionRegistry;
  private final LogicalSchema schema;
  private final KsqlConfig ksqlConfig;
  private final Expression expression;
  private final SqlType expressionType;

  public ExpressionInterpreter(final FunctionRegistry functionRegistry,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final Expression expression,
      final SqlType expressionType) {
    this.ksqlConfig = ksqlConfig;
    this.expression = expression;
    this.expressionType = expressionType;
    this.expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);
    this.functionRegistry = functionRegistry;
    this.schema = schema;
  }

  public Object evaluate(final GenericRow row) {
    final Pair<Object, SqlType> evaluator =
        new Evaluator(row).process(expression, null);
    return evaluator.getLeft();
  }

  @Override
  public Object evaluate(
      final GenericRow row,
      final Object defaultValue,
      final ProcessingLogger logger,
      final Supplier<String> errorMsg
  ) {
    try {
      return evaluate(row);
    } catch (final KsqlException e) {
      // This is likely something that would have been caught previously at compile time, so allow
      // it to bubble up.
      throw e;
    } catch (final Exception e) {
      logger.error(RecordProcessingError.recordProcessingError(errorMsg.get(), e, row));
      return defaultValue;
    }
  }

  @Override
  public Expression getExpression() {
    return null;
  }

  public SqlType getExpressionType() {
    return expressionType;
  }

  private class Evaluator implements ExpressionVisitor<Pair<Object, SqlType>, Void> {

    private final GenericRow row;

    Evaluator(final GenericRow row) {
      this.row = row;
    }

    private Pair<Object, SqlType> visitIllegalState(final Expression expression) {
      throw new IllegalStateException(
          format("expression type %s should never be visited", expression.getClass()));
    }

    private Pair<Object, SqlType> visitUnsupported(final Expression expression) {
      throw new UnsupportedOperationException(
          format(
              "not yet implemented: %s.visit%s",
              getClass().getName(),
              expression.getClass().getSimpleName()
          )
      );
    }

    @Override
    public Pair<Object, SqlType> visitType(final Type node, final Void context) {
      return visitIllegalState(node);
    }

    @Override
    public Pair<Object, SqlType> visitWhenClause(final WhenClause whenClause, final Void context) {
      return visitIllegalState(whenClause);
    }

    @Override
    public Pair<Object, SqlType> visitInPredicate(
        final InPredicate inPredicate,
        final Void context
    ) {
      return visitUnsupported(inPredicate);
    }

    @Override
    public Pair<Object, SqlType> visitInListExpression(
        final InListExpression inListExpression, final Void context
    ) {
      return visitUnsupported(inListExpression);
    }

    @Override
    public Pair<Object, SqlType> visitTimestampLiteral(
        final TimestampLiteral node, final Void context
    ) {
      return new Pair<>(node.getValue(), SqlTypes.TIMESTAMP);
    }

    @Override
    public Pair<Object, SqlType> visitTimeLiteral(
        final TimeLiteral timeLiteral,
        final Void context
    ) {
      return visitUnsupported(timeLiteral);
    }

    @Override
    public Pair<Object, SqlType> visitSimpleCaseExpression(
        final SimpleCaseExpression simpleCaseExpression, final Void context
    ) {
      return visitUnsupported(simpleCaseExpression);
    }

    @Override
    public Pair<Object, SqlType> visitBooleanLiteral(
        final BooleanLiteral node,
        final Void context
    ) {
      return new Pair<>(node.getValue(), SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<Object, SqlType> visitStringLiteral(final StringLiteral node, final Void context) {
      return new Pair<>(node.getValue(), SqlTypes.STRING);
    }

    @Override
    public Pair<Object, SqlType> visitDoubleLiteral(final DoubleLiteral node, final Void context) {
      return new Pair<>(node.getValue(), SqlTypes.DOUBLE);
    }

    @Override
    public Pair<Object, SqlType> visitDecimalLiteral(
        final DecimalLiteral decimalLiteral,
        final Void context
    ) {
      return new Pair<>(
          decimalLiteral.getValue(),
          DecimalUtil.fromValue(decimalLiteral.getValue())
      );
    }

    @Override
    public Pair<Object, SqlType> visitNullLiteral(final NullLiteral node, final Void context) {
      return new Pair<>(null, null);
    }

    @Override
    public Pair<Object, SqlType> visitLambdaExpression(
        final LambdaFunctionCall lambdaFunctionCall, final Void context) {
      return visitUnsupported(lambdaFunctionCall);
    }

    @Override
    public Pair<Object, SqlType> visitLambdaVariable(
        final LambdaVariable lambdaLiteral, final Void context) {
      return visitUnsupported(lambdaLiteral);
    }

    @Override
    public Pair<Object, SqlType> visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      final Column schemaColumn = schema.findValueColumn(node.getColumnName())
          .orElseThrow(() ->
              new KsqlException("Field not found: " + node.getColumnName()));

      final Object value = row.get(schemaColumn.index());

      return new Pair<>(value, schemaColumn.type());
    }

    @Override
    public Pair<Object, SqlType> visitQualifiedColumnReference(
        final QualifiedColumnReferenceExp node,
        final Void context
    ) {
      throw new UnsupportedOperationException(
          "Qualified column reference must be resolved to unqualified reference before codegen"
      );
    }

    @Override
    public Pair<Object, SqlType> visitDereferenceExpression(
        final DereferenceExpression node, final Void context
    ) {
      final SqlType functionReturnSchema = expressionTypeManager.getExpressionSqlType(node);

      final Pair<Object, SqlType> base = process(node.getBase(), context);

      if (base.getRight().baseType() != SqlBaseType.STRUCT) {
        throw new KsqlException("Can only dereference Struct type, instead got " + base.getRight());
      }

      final Struct struct = (Struct) base.getLeft();
      final String field = (String) process(new StringLiteral(node.getFieldName()), context)
          .getLeft();
      final Object dereference = struct.get(field);

      return new Pair<>(dereference, functionReturnSchema);
    }

    public Pair<Object, SqlType> visitLongLiteral(final LongLiteral node, final Void context) {
      return new Pair<>(node.getValue(), SqlTypes.BIGINT);
    }

    @Override
    public Pair<Object, SqlType> visitIntegerLiteral(
        final IntegerLiteral node,
        final Void context
    ) {
      return new Pair<>(node.getValue(), SqlTypes.INTEGER);
    }

    @Override
    public Pair<Object, SqlType> visitFunctionCall(final FunctionCall node, final Void context) {
      final UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName());
      final List<SqlType> argumentSchemas = node.getArguments().stream()
          .map(expressionTypeManager::getExpressionSqlType)
          .collect(Collectors.toList());

      final KsqlScalarFunction function = udfFactory.getFunction(argumentSchemas);

      final SqlType functionReturnSchema = function.getReturnType(argumentSchemas);
      final Class<?> javaClass =
          SchemaConverters.sqlToJavaConverter().toJavaType(functionReturnSchema);

      final List<Expression> arguments = node.getArguments();

      final List<Object> args = new ArrayList<>();
      for (int i = 0; i < arguments.size(); i++) {
        final Expression arg = arguments.get(i);
        final SqlType sqlType = argumentSchemas.get(i);

        final ParamType paramType;
        if (i >= function.parameters().size() - 1 && function.isVariadic()) {
          paramType = ((ArrayType) Iterables.getLast(function.parameters())).element();
        } else {
          paramType = function.parameters().get(i);
        }

        // This will attempt to cast to the expected argument type and will throw an error if
        // it cannot be done.
        final Object argJava = process(convertArgument(arg, sqlType, paramType), context).getLeft();
        args.add(argJava);
      }

      final Kudf kudf = function.newInstance(ksqlConfig);
      final Object result = kudf.evaluate(args.toArray());
      final Object castedResult = javaClass.cast(result);
      return new Pair<>(castedResult, functionReturnSchema);
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
    public Pair<Object, SqlType> visitLogicalBinaryExpression(
        final LogicalBinaryExpression node, final Void context
    ) {
      final Pair<Object, SqlType> left = process(node.getLeft(), context);
      final Pair<Object, SqlType> right = process(node.getRight(), context);
      if (!(left.getRight().baseType() == SqlBaseType.BOOLEAN
          && left.getRight().baseType() == SqlBaseType.BOOLEAN)) {
        throw new KsqlException(
            format("Logical binary expects two boolean values.  Actual %s and %s",
                left.getRight(), right.getRight()));
      }

      final Boolean leftBoolean = (Boolean) left.getLeft();
      final Boolean rightBoolean = (Boolean) right.getLeft();

      if (node.getType() == LogicalBinaryExpression.Type.OR) {
        return new Pair<>(
            leftBoolean || rightBoolean,
            SqlTypes.BOOLEAN
        );
      } else if (node.getType() == LogicalBinaryExpression.Type.AND) {
        return new Pair<>(
            leftBoolean && rightBoolean,
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
    public Pair<Object, SqlType> visitNotExpression(final NotExpression node, final Void context) {
      final Pair<Object, SqlType> pair = process(node.getValue(), context);
      if (!(pair.getRight().baseType() == SqlBaseType.BOOLEAN)) {
        throw new IllegalStateException(
            format("Not expression expects a boolean value.  Actual %s", pair.getRight()));
      }

      final Boolean exprBoolean = (Boolean) pair.getLeft();
      return new Pair<>(!exprBoolean, SqlTypes.BOOLEAN);
    }

    private Boolean nullCheckQuickReturn(
        final ComparisonExpression.Type type, final Object left, final Object right) {
      if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
        return (left == null || right == null)
            ? ((left == null) ^ ((right) == null)) : null;
      }
      return (left == null || right == null) ? false : null;
    }

    @Override
    public Pair<Object, SqlType> visitComparisonExpression(
        final ComparisonExpression node, final Void context
    ) {
      final Pair<Object, SqlType> left = process(node.getLeft(), context);
      final Pair<Object, SqlType> right = process(node.getRight(), context);
      final SqlBaseType leftType = left.getRight().baseType();
      final Object leftObject = left.getLeft();
      final Object rightObject = right.getLeft();

      final Boolean nullCheck = nullCheckQuickReturn(node.getType(), leftObject, rightObject);
      if (nullCheck != null) {
        return new Pair<>(nullCheck, SqlTypes.BOOLEAN);
      }

      final Integer compareTo = doNumericalCompareTo(left, right);
      if (compareTo != null) {
        return new Pair<>(doComparisonCheck(node, compareTo), SqlTypes.BOOLEAN);
      }
      Boolean equals = null;
      if (leftType == SqlBaseType.ARRAY
          || leftType == SqlBaseType.MAP
          || leftType == SqlBaseType.STRUCT
          || leftType == SqlBaseType.BOOLEAN) {
        equals = left.equals(right);
      }
      if (equals != null) {
        return new Pair<>(doEqualsCheck(left.getRight(), node, equals), SqlTypes.BOOLEAN);
      }
      throw new KsqlException("Unknown types for " + left + " and " + right);
    }

    @Override
    public Pair<Object, SqlType> visitCast(final Cast node, final Void context) {
      final Pair<Object, SqlType> expr = process(node.getExpression(), context);
      final SqlType from = expr.right;
      final SqlType to = node.getType().getSqlType();
      return new Pair<>(
          CastInterpreter.cast(expr.left, from, to, ksqlConfig),
          to);
    }

    @Override
    public Pair<Object, SqlType> visitIsNullPredicate(
        final IsNullPredicate node,
        final Void context
    ) {
      final Pair<Object, SqlType> value = process(node.getValue(), context);
      return new Pair<>(value.getLeft() == null, SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<Object, SqlType> visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final Void context
    ) {
      final Pair<Object, SqlType> value = process(node.getValue(), context);
      return new Pair<>(value.getLeft() != null, SqlTypes.BOOLEAN);
    }

    @Override
    public Pair<Object, SqlType> visitArithmeticUnary(
        final ArithmeticUnaryExpression node, final Void context
    ) {
      final Pair<Object, SqlType> value = process(node.getValue(), context);
      switch (node.getSign()) {
        case MINUS:
          return visitArithmeticMinus(value);
        case PLUS:
          return visitArithmeticPlus(value);
        default:
          throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
      }
    }

    private Pair<Object, SqlType> visitArithmeticMinus(final Pair<Object, SqlType> value) {
      if (value.getLeft() instanceof BigDecimal) {
        final BigDecimal bigDecimal = (BigDecimal) value.getLeft();
        return new Pair<>(
            bigDecimal.negate(new MathContext(((SqlDecimal) value.getRight()).getPrecision(),
                RoundingMode.UNNECESSARY)),
            value.getRight()
        );
      } else if (value.getRight().baseType() == SqlBaseType.DOUBLE) {
        final double val = (Double) value.getLeft();
        return new Pair<>(-val, value.getRight());
      } else if (value.getRight().baseType() == SqlBaseType.INTEGER) {
        final int val = (Integer) value.getLeft();
        return new Pair<>(-val, value.getRight());
      } else if (value.getRight().baseType() == SqlBaseType.BIGINT) {
        final long val = (Long) value.getLeft();
        return new Pair<>(-val, value.getRight());
      } else {
        throw new UnsupportedOperationException("Negation on unsupported type: "
            + value.getLeft());
      }
    }

    private Pair<Object, SqlType> visitArithmeticPlus(final Pair<Object, SqlType> value) {
      if (value.getRight().baseType() == SqlBaseType.DECIMAL) {
        final BigDecimal bigDecimal = (BigDecimal) value.getLeft();
        return new Pair<>(
            bigDecimal.plus(new MathContext(((SqlDecimal) value.getRight()).getPrecision(),
                RoundingMode.UNNECESSARY)),
            value.getRight()
        );
      } else if (value.getRight().baseType() == SqlBaseType.DOUBLE) {
        final double val = (Double) value.getLeft();
        return new Pair<>(+val, value.getRight());
      } else if (value.getRight().baseType() == SqlBaseType.INTEGER) {
        final int val = (Integer) value.getLeft();
        return new Pair<>(+val, value.getRight());
      } else if (value.getRight().baseType() == SqlBaseType.BIGINT) {
        final long val = (Long) value.getLeft();
        return new Pair<>(+val, value.getRight());
      } else {
        throw new UnsupportedOperationException("Unary plus on unsupported type: "
            + value.getLeft());
      }
    }

    @Override
    public Pair<Object, SqlType> visitArithmeticBinary(
        final ArithmeticBinaryExpression node, final Void context
    ) {
      final Pair<Object, SqlType> left = process(node.getLeft(), context);
      final Pair<Object, SqlType> right = process(node.getRight(), context);

      final SqlType schema = expressionTypeManager.getExpressionSqlType(node);

      if (schema.baseType() == SqlBaseType.DECIMAL) {
        final SqlDecimal decimal = (SqlDecimal) schema;
        final BigDecimal leftExpr = (BigDecimal) CastInterpreter.cast(left.left, left.right,
            DecimalUtil.toSqlDecimal(left.right), ksqlConfig);
        final BigDecimal rightExpr = (BigDecimal) CastInterpreter.cast(right.left, right.right,
            DecimalUtil.toSqlDecimal(right.right), ksqlConfig);
        return new Pair<>(
            ArithmeticInterpreter.apply(decimal, node.getOperator(), leftExpr, rightExpr),
            schema);
      } else {
        final Object leftObject =
            left.getRight().baseType() == SqlBaseType.DECIMAL
                ? CastInterpreter.cast(left.left, left.right, SqlTypes.DOUBLE, ksqlConfig)
                : left.getLeft();
        final Object rightObject =
            right.getRight().baseType() == SqlBaseType.DECIMAL
                ? CastInterpreter.cast(right.left, right.right, SqlTypes.DOUBLE, ksqlConfig)
                : right.getLeft();

        final Object result = ArithmeticInterpreter.doArithmetic(
            node, left.getRight(), right.getRight(), leftObject, rightObject);
        return new Pair<>(result, schema);
      }
    }

    @Override
    public Pair<Object, SqlType> visitSearchedCaseExpression(
        final SearchedCaseExpression node, final Void context
    ) {
      final SqlType resultSchema = expressionTypeManager.getExpressionSqlType(node);

      final List<LazyWhenClause<Object>> lazyWhenClause = node
          .getWhenClauses()
          .stream()
          .map(whenClause -> SearchedCaseFunction.whenClause(
              () -> (Boolean) process(whenClause.getOperand(), context).getLeft(),
              () -> process(whenClause.getResult(), context).getLeft()))
          .collect(ImmutableList.toImmutableList());

      final Object defaultValue = node.getDefaultValue().isPresent()
          ? process(node.getDefaultValue().get(), context).getLeft()
          : null;

      final Object result =
          SearchedCaseFunction.searchedCaseFunction(lazyWhenClause, () -> defaultValue);

      return new Pair<>(result, resultSchema);
    }

    @Override
    public Pair<Object, SqlType> visitLikePredicate(final LikePredicate node, final Void context) {
      final String patternString = (String) process(node.getPattern(), context).getLeft();
      final String valueString = (String) process(node.getValue(), context).getLeft();

      if (node.getEscape().isPresent()) {
        return new Pair<>(
            LikeEvaluator.matches(valueString, patternString, node.getEscape().get()),
            SqlTypes.STRING
        );
      } else {
        return new Pair<>(
            LikeEvaluator.matches(valueString, patternString),
            SqlTypes.STRING
        );
      }
    }

    @Override
    public Pair<Object, SqlType> visitSubscriptExpression(
        final SubscriptExpression node,
        final Void context
    ) {
      final SqlType internalSchema = expressionTypeManager.getExpressionSqlType(node.getBase());
      switch (internalSchema.baseType()) {
        case ARRAY:
          final SqlArray array = (SqlArray) internalSchema;
          final List<?> list = (List<?>) process(node.getBase(), context).getLeft();
          final Integer suppliedIdx = (Integer) process(node.getIndex(), context).getLeft();
          final Object object = ArrayAccess.arrayAccess(list, suppliedIdx);
          return new Pair<>(object, array.getItemType());

        case MAP:
          final SqlMap mapSchema = (SqlMap) internalSchema;
          final Map<?, ?> map = (Map<?, ?>) process(node.getBase(), context).getLeft();
          final Object key = process(node.getIndex(), context).getLeft();
          final Object mapValue = map.get(key);
          return new Pair<>(
              mapValue,
              mapSchema.getValueType()
          );

        default:
          throw new UnsupportedOperationException();
      }
    }

    @Override
    public Pair<Object, SqlType> visitCreateArrayExpression(
        final CreateArrayExpression exp,
        final Void context
    ) {
      final List<Expression> expressions = CoercionUtil
          .coerceUserList(exp.getValues(), expressionTypeManager)
          .expressions();

      final ArrayBuilder build = new ArrayBuilder(expressions.size());

      for (Expression value : expressions) {
        build.add(process(value, context).getLeft());
      }
      return new Pair<>(
          build.build(),
          expressionTypeManager.getExpressionSqlType(exp));
    }

    @Override
    public Pair<Object, SqlType> visitCreateMapExpression(
        final CreateMapExpression exp,
        final Void context
    ) {
      final ImmutableMap<Expression, Expression> map = exp.getMap();
      final List<Expression> keys = CoercionUtil
          .coerceUserList(map.keySet(), expressionTypeManager)
          .expressions();

      final List<Expression> values = CoercionUtil
          .coerceUserList(map.values(), expressionTypeManager)
          .expressions();

      final MapBuilder builder = new MapBuilder(map.size());

      final Iterable<Pair<Expression, Expression>> pairs = () -> Streams.zip(
          keys.stream(), values.stream(), Pair::of)
          .iterator();
      for (Pair<Expression, Expression> p : pairs) {
        builder.put(
            process(p.getLeft(), context).getLeft(),
            process(p.getRight(), context).getLeft());
      }

      return new Pair<>(
          builder.build(),
          expressionTypeManager.getExpressionSqlType(exp));
    }

    @Override
    public Pair<Object, SqlType> visitStructExpression(
        final CreateStructExpression node,
        final Void context
    ) {
      final Schema schema = SchemaConverters
          .sqlToConnectConverter()
          .toConnectSchema(expressionTypeManager.getExpressionSqlType(node));
      final Struct struct = new Struct(schema);
      for (final Field field : node.getFields()) {
        struct.put(field.getName(), process(field.getValue(), context).getLeft());
      }
      return new Pair<>(
          struct,
          expressionTypeManager.getExpressionSqlType(node)
      );
    }

    @Override
    public Pair<Object, SqlType> visitBetweenPredicate(
        final BetweenPredicate node,
        final Void context
    ) {
      final Pair<Object, SqlType> compareMin = process(
          new ComparisonExpression(
              ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
              node.getValue(),
              node.getMin()),
          context);
      final Pair<Object, SqlType> compareMax = process(
          new ComparisonExpression(
              ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
              node.getValue(),
              node.getMax()),
          context);

      return new Pair<>(
          ((Boolean) compareMin.getLeft()) && ((Boolean) compareMax.getLeft()),
          SqlTypes.BOOLEAN
      );
    }
  }
}
