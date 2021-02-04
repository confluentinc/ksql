package io.confluent.ksql.execution.evaluator;

import static io.confluent.ksql.schema.ksql.SchemaConverters.sqlToFunctionConverter;
import static java.lang.String.format;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.helpers.CastEvaluator;
import io.confluent.ksql.execution.expression.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.execution.expression.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.execution.expression.tree.BetweenPredicate;
import io.confluent.ksql.execution.expression.tree.BooleanLiteral;
import io.confluent.ksql.execution.expression.tree.Cast;
import io.confluent.ksql.execution.expression.tree.ComparisonExpression;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
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
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaLiteral;
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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlTimestamps;
import io.confluent.ksql.schema.ksql.types.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.SqlDecimal;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

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
  public Object evaluate(GenericRow row, Object defaultValue,
      ProcessingLogger logger, Supplier<String> errorMsg) {
    try {
      return evaluate(row);
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
    public Pair<Object, SqlType> visitLambdaLiteral(
        final LambdaLiteral lambdaLiteral, final Void context) {
      return visitUnsupported(lambdaLiteral);
    }

    @Override
    public Pair<Object, SqlType> visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      final ColumnName fieldName = node.getColumnName();
      final Column schemaColumn = schema.findValueColumn(node.getColumnName())
          .orElseThrow(() ->
              new KsqlException("Field not found: " + node.getColumnName()));

      Object value = row.get(schemaColumn.index());

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
//      final SqlType functionReturnSchema = expressionTypeManager.getExpressionSqlType(node);
//      final Class<?> javaReturnType =
//          SchemaConverters.sqlToJavaConverter().toJavaType(functionReturnSchema);
//
//      final Object struct = process(node.getBase(), context).getLeft();
//      final Object field = process(new StringLiteral(node.getFieldName()), context).getLeft();
//      final String codeString = "((" + javaReturnType + ") "
//          + struct + ".get(" + field + "))";
//
//      return new Pair<>(codeString, functionReturnSchema);
      return visitUnsupported(node);
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
      final List<SqlType> sqlTypes = new ArrayList<>();
      for (int i = 0; i < arguments.size(); i++) {
        final Expression arg = arguments.get(i);
        final SqlType sqlType = argumentSchemas.get(i);

        final ParamType paramType;
        if (i >= function.parameters().size() - 1 && function.isVariadic()) {
          paramType = ((ArrayType) Iterables.getLast(function.parameters())).element();
        } else {
          paramType = function.parameters().get(i);
        }

        Object argJava = process(convertArgument(arg, sqlType, paramType), context).getLeft();
        args.add(argJava);
        sqlTypes.add(sqlType);
      }

      Kudf kudf = function.newInstance(ksqlConfig);
      Object result = kudf.evaluate(args.toArray());
      Object castedResult = javaClass.cast(result);
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
      Object left = process(node.getLeft(), context).getLeft();
      Object right = process(node.getRight(), context).getLeft();
      if (!(left instanceof Boolean && right instanceof Boolean)) {
        throw new IllegalStateException(
            format("Logical binary expects two boolean values.  Actual %s (%s) and %s (%s)",
                left, left.getClass().getName(), right, right.getClass().getName()));
      }

      Boolean leftBoolean = (Boolean) left;
      Boolean rightBoolean = (Boolean) right;
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
      final Object expr = process(node.getValue(), context).getLeft();
      if (!(expr instanceof Boolean)) {
        throw new IllegalStateException(
            format("Not expression expects a boolean value.  Actual %s (%s)",
                expr, expr.getClass().getName()));
      }
      Boolean exprBoolean = (Boolean) expr;
      return new Pair<>(!exprBoolean, SqlTypes.BOOLEAN);
    }

    private Boolean nullCheckQuickReturn(
        final ComparisonExpression.Type type, Object left, Object right) {
      if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
        return (left == null || right == null) ?
             ((left == null ) ^ ((right) == null )) : null;
      }
      return (left == null || right == null) ? false : null;
    }

    private Double toDouble(final Object object) {
      if (object instanceof Double) {
        return (Double) object;
      } else if (object instanceof Long) {
        return ((Long) object).doubleValue();
      } else if (object instanceof Integer) {
        return ((Integer) object).doubleValue();
      } else {
        throw new KsqlException("Unexpected type conversion to Double: " + object);
      }
    }

    private Long toLong(final Object object) {
      if (object instanceof Double) {
        return ((Double) object).longValue();
      } else if (object instanceof Long) {
        return ((Long) object);
      } else if (object instanceof Integer) {
        return ((Integer) object).longValue();
      } else {
        throw new KsqlException("Unexpected type conversion to Long: " + object);
      }
    }

    private Integer toInteger(final Object object) {
      if (object instanceof Double) {
        return ((Double) object).intValue();
      } else if (object instanceof Long) {
        return ((Long) object).intValue();
      } else if (object instanceof Integer) {
        return ((Integer) object);
      } else {
        throw new KsqlException("Unexpected type conversion to Integer: " + object);
      }
    }

    private Boolean toBoolean(final Object object) {
      if (object instanceof Boolean) {
        return ((Boolean) object);
      } else {
        throw new KsqlException("Unexpected type conversion to Boolean: " + object);
      }
    }

//    private boolean visitDecimalComparisonExpression(final Object left, final Object right) {
//      int compareTo = toDecimal(left).compareTo(toDecimal(right));
//
//    }



    private BigDecimal toDecimal(final Object object) {
      if (object instanceof BigDecimal) {
        return (BigDecimal) object;
      } else if (object instanceof Double) {
        return BigDecimal.valueOf((Double) object);
      } else if (object instanceof Integer) {
        return new BigDecimal((Integer) object);
      } else if (object instanceof Long) {
        return new BigDecimal((Long) object);
      } else if (object instanceof String) {
        return new BigDecimal((String) object);
      } else {
        throw new KsqlException("Unexpected type convertion to BigDecimal: " + object);
      }
    }

    private Timestamp toTimestamp(final Object object) {
      if (object instanceof Timestamp) {
        return (Timestamp) object;
      } else if (object instanceof String) {
        return SqlTimestamps.parseTimestamp((String) object);
      } else {
        throw new KsqlException("Unexpected comparison to TIMESTAMP: " + object);
      }
    }

    private boolean doComparisonCheck(final ComparisonExpression node, int compareTo) {
        switch (node.getType()) {
          case EQUAL:
            return compareTo == 0;
          case NOT_EQUAL:
          case IS_DISTINCT_FROM:
            return compareTo != 0;
          case GREATER_THAN_OR_EQUAL:
            return compareTo >= 0;
          case GREATER_THAN:
            return compareTo > 0;
          case LESS_THAN_OR_EQUAL:
            return compareTo <= 0;
          case LESS_THAN:
            return compareTo < 0;
          default:
            throw new KsqlException("Unexpected scalar comparison: " + node.getType().getValue());
        }
    }

    private boolean doEqualsCheck(final Class clazz, final ComparisonExpression node,
        boolean equals) {
      switch (node.getType()) {
        case EQUAL:
          return equals;
        case NOT_EQUAL:
        case IS_DISTINCT_FROM:
          return !equals;
        default:
          throw new KsqlException("Unexpected " + clazz.getName()  + " comparison: "
              + node.getType().getValue());
      }
    }


    @Override
    public Pair<Object, SqlType> visitComparisonExpression(
        final ComparisonExpression node, final Void context
    ) {
      final Pair<Object, SqlType> left = process(node.getLeft(), context);
      final Pair<Object, SqlType> right = process(node.getRight(), context);

      Boolean nullCheck = nullCheckQuickReturn(node.getType(), left, right);
      if (nullCheck != null) {
        return new Pair<>(nullCheck, SqlTypes.BOOLEAN);
      }

      Integer compareTo = null;
      if (left.getRight().baseType() == SqlBaseType.DECIMAL
          || right.getRight().baseType() == SqlBaseType.DECIMAL) {
        compareTo = toDecimal(left.getLeft()).compareTo(toDecimal(right.getLeft()));
      } else if (left.getRight().baseType() == SqlBaseType.TIMESTAMP
          || right.getRight().baseType() == SqlBaseType.TIMESTAMP) {
        compareTo = toTimestamp(left.getLeft()).compareTo(toTimestamp(right.getLeft()));
      } else if (left.getLeft() instanceof String) {
        compareTo = left.getLeft().toString().compareTo(right.getLeft().toString());
      } else if (left.getLeft() instanceof Double || right.getLeft() instanceof Double) {
        compareTo = toDouble(left.getLeft()).compareTo(toDouble(right.getLeft()));
      } else if (left.getLeft() instanceof Long || right.getLeft() instanceof Long) {
        compareTo = toLong(left.getLeft()).compareTo(toLong(right.getLeft()));
      } else if (left.getLeft() instanceof Integer || right.getLeft() instanceof Integer) {
        compareTo = toInteger(left.getLeft()).compareTo(toInteger(right.getLeft()));
      }
      if (compareTo != null) {
        return new Pair<>(doComparisonCheck(node, compareTo), SqlTypes.BOOLEAN);
      }
      Boolean equals = null;
      if (left.getLeft() instanceof List
          || left.getLeft() instanceof Map
          || left.getLeft() instanceof Boolean ) {
        equals = left.equals(right);
      }
      if (equals != null) {
        return new Pair<>(doEqualsCheck(left.getLeft().getClass(), node, equals), SqlTypes.BOOLEAN);
      }
      throw new KsqlException("Unknown types for " + left  + " and " + right);
    }

    @Override
    public Pair<Object, SqlType> visitCast(final Cast node, final Void context) {
//      final Pair<String, SqlType> expr = process(node.getExpression(), context);
//      final SqlType to = node.getType().getSqlType();
//      return Pair.of(genCastCode(expr, to), to);
      return visitUnsupported(node);
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
      } else {
        if (value.getLeft() instanceof Double) {
          double val = (Double) value.getLeft();
          return new Pair<>(-val, value.getRight());
        } else if (value.getLeft() instanceof Integer) {
          int val = (Integer) value.getLeft();
          return new Pair<>(-val, value.getRight());
        } else if (value.getLeft() instanceof Long) {
          long val = (Long) value.getLeft();
          return new Pair<>(-val, value.getRight());
        } else {
          throw new UnsupportedOperationException("Negation on unsupported type: "
              + value.getLeft());
        }
      }
    }

    private Pair<Object, SqlType> visitArithmeticPlus(final Pair<Object, SqlType> value) {
      if (value.getLeft() instanceof BigDecimal) {
        final BigDecimal bigDecimal = (BigDecimal) value.getLeft();
        return new Pair<>(
            bigDecimal.plus(new MathContext(((SqlDecimal) value.getRight()).getPrecision(),
                RoundingMode.UNNECESSARY)),
            value.getRight()
        );
      } else {
        if (value.getLeft() instanceof Double) {
          double val = (Double) value.getLeft();
          return new Pair<>(+val, value.getRight());
        } else if (value.getLeft() instanceof Integer) {
          int val = (Integer) value.getLeft();
          return new Pair<>(+val, value.getRight());
        } else if (value.getLeft() instanceof Long) {
          long val = (Long) value.getLeft();
          return new Pair<>(+val, value.getRight());
        } else {
          throw new UnsupportedOperationException("Unary plus on unsupported type: "
              + value.getLeft());
        }
      }
    }

    @Override
    public Pair<Object, SqlType> visitArithmeticBinary(
        final ArithmeticBinaryExpression node, final Void context
    ) {
//      final Pair<Object, SqlType> left = process(node.getLeft(), context);
//      final Pair<Object, SqlType> right = process(node.getRight(), context);
//
//      final SqlType schema = expressionTypeManager.getExpressionSqlType(node);
//
//      if (schema.baseType() == SqlBaseType.DECIMAL) {
//        final SqlDecimal decimal = (SqlDecimal) schema;
//        final String leftExpr = genCastCode(left, DecimalUtil.toSqlDecimal(left.right));
//        final String rightExpr = genCastCode(right, DecimalUtil.toSqlDecimal(right.right));
//
//        return new Pair<>(
//            String.format(
//                "(%s.%s(%s, new MathContext(%d, RoundingMode.UNNECESSARY)).setScale(%d))",
//                leftExpr,
//                DECIMAL_OPERATOR_NAME.get(node.getOperator()),
//                rightExpr,
//                decimal.getPrecision(),
//                decimal.getScale()
//            ),
//            schema
//        );
//      } else {
//        final String leftExpr =
//            left.getRight().baseType() == SqlBaseType.DECIMAL
//                ? genCastCode(left, SqlTypes.DOUBLE)
//                : left.getLeft();
//        final String rightExpr =
//            right.getRight().baseType() == SqlBaseType.DECIMAL
//                ? genCastCode(right, SqlTypes.DOUBLE)
//                : right.getLeft();
//
//        return new Pair<>(
//            String.format(
//                "(%s %s %s)",
//                leftExpr,
//                node.getOperator().getSymbol(),
//                rightExpr
//            ),
//            schema
//        );
//      }
      return visitUnsupported(node);
    }

    @Override
    public Pair<Object, SqlType> visitSearchedCaseExpression(
        final SearchedCaseExpression node, final Void context
    ) {
//      final String functionClassName = SearchedCaseFunction.class.getSimpleName();
//      final List<CaseWhenProcessed> whenClauses = node
//          .getWhenClauses()
//          .stream()
//          .map(whenClause -> new CaseWhenProcessed(
//              process(whenClause.getOperand(), context),
//              process(whenClause.getResult(), context)
//          ))
//          .collect(Collectors.toList());
//
//      final SqlType resultSchema = expressionTypeManager.getExpressionSqlType(node);
//      final String resultSchemaString =
//          SchemaConverters.sqlToJavaConverter().toJavaType(resultSchema).getCanonicalName();
//
//      final List<String> lazyWhenClause = whenClauses
//          .stream()
//          .map(processedWhenClause -> functionClassName + ".whenClause("
//              + buildSupplierCode(
//              "Boolean", processedWhenClause.whenProcessResult.getLeft())
//              + ", "
//              + buildSupplierCode(
//              resultSchemaString, processedWhenClause.thenProcessResult.getLeft())
//              + ")")
//          .collect(Collectors.toList());
//
//      final String defaultValue = node.getDefaultValue().isPresent()
//          ? process(node.getDefaultValue().get(), context).getLeft()
//          : "null";
//
//      // ImmutableList.copyOf(Arrays.asList()) replaced ImmutableList.of() to avoid
//      // CASE expressions with 12+ conditions from breaking. Don't change it unless
//      // you are certain it won't break it. See https://github.com/confluentinc/ksql/issues/5707
//      final String codeString = "((" + resultSchemaString + ")"
//          + functionClassName + ".searchedCaseFunction(ImmutableList.copyOf(Arrays.asList( "
//          + StringUtils.join(lazyWhenClause, ", ") + ")),"
//          + buildSupplierCode(resultSchemaString, defaultValue)
//          + "))";
//
//      return new Pair<>(codeString, resultSchema);
      return visitUnsupported(node);
    }

    private String buildSupplierCode(final String typeString, final String code) {
      return " new " + Supplier.class.getSimpleName() + "<" + typeString + ">() {"
          + " @Override public " + typeString + " get() { return " + code + "; }}";
    }

    @Override
    public Pair<Object, SqlType> visitLikePredicate(final LikePredicate node, final Void context) {

//      final String patternString = process(node.getPattern(), context).getLeft();
//      final String valueString = process(node.getValue(), context).getLeft();
//
//      if (node.getEscape().isPresent()) {
//        return new Pair<>(
//            "LikeEvaluator.matches("
//                + valueString + ", "
//                + patternString + ", '"
//                + node.getEscape().get() + "')",
//            SqlTypes.STRING
//        );
//      } else {
//        return new Pair<>(
//            "LikeEvaluator.matches(" + valueString + ", " + patternString + ")",
//            SqlTypes.STRING
//        );
//      }
      return visitUnsupported(node);
    }

    @Override
    public Pair<Object, SqlType> visitSubscriptExpression(
        final SubscriptExpression node,
        final Void context
    ) {
//      final SqlType internalSchema = expressionTypeManager.getExpressionSqlType(node.getBase());
//
//      final String internalSchemaJavaType =
//          SchemaConverters.sqlToJavaConverter().toJavaType(internalSchema).getCanonicalName();
//      switch (internalSchema.baseType()) {
//        case ARRAY:
//          final SqlArray array = (SqlArray) internalSchema;
//          final String listName = process(node.getBase(), context).getLeft();
//          final String suppliedIdx = process(node.getIndex(), context).getLeft();
//
//          final String code = format(
//              "((%s) (%s.arrayAccess((%s) %s, ((int) %s))))",
//              SchemaConverters.sqlToJavaConverter().toJavaType(array.getItemType()).getSimpleName(),
//              ArrayAccess.class.getSimpleName(),
//              internalSchemaJavaType,
//              listName,
//              suppliedIdx
//          );
//
//          return new Pair<>(code, array.getItemType());
//
//        case MAP:
//          final SqlMap map = (SqlMap) internalSchema;
//          return new Pair<>(
//              String.format(
//                  "((%s) ((%s)%s).get(%s))",
//                  SchemaConverters.sqlToJavaConverter()
//                      .toJavaType(map.getValueType()).getSimpleName(),
//                  internalSchemaJavaType,
//                  process(node.getBase(), context).getLeft(),
//                  process(node.getIndex(), context).getLeft()
//              ),
//              map.getValueType()
//          );
//        default:
//          throw new UnsupportedOperationException();
//      }
      return visitUnsupported(node);
    }

    @Override
    public Pair<Object, SqlType> visitCreateArrayExpression(
        final CreateArrayExpression exp,
        final Void context
    ) {
//      final List<Expression> expressions = CoercionUtil
//          .coerceUserList(exp.getValues(), expressionTypeManager)
//          .expressions();
//
//      final StringBuilder array = new StringBuilder("new ArrayBuilder(");
//      array.append(expressions.size());
//      array.append((')'));
//
//      for (Expression value : expressions) {
//        array.append(".add(");
//        array.append(process(value, context).getLeft());
//        array.append(")");
//      }
//      return new Pair<>(
//          "((List)" + array.toString() + ".build())",
//          expressionTypeManager.getExpressionSqlType(exp));
      return visitUnsupported(exp);
    }

    @Override
    public Pair<Object, SqlType> visitCreateMapExpression(
        final CreateMapExpression exp,
        final Void context
    ) {
//      final ImmutableMap<Expression, Expression> map = exp.getMap();
//      final List<Expression> keys = CoercionUtil
//          .coerceUserList(map.keySet(), expressionTypeManager)
//          .expressions();
//
//      final List<Expression> values = CoercionUtil
//          .coerceUserList(map.values(), expressionTypeManager)
//          .expressions();
//
//      final String entries = Streams.zip(
//          keys.stream(),
//          values.stream(),
//          (k, v) -> ".put(" + process(k, context).getLeft() + ", " + process(v, context).getLeft()
//              + ")"
//      ).collect(Collectors.joining());
//
//      return new Pair<>(
//          "((Map)new MapBuilder(" + map.size() + ")" + entries + ".build())",
//          expressionTypeManager.getExpressionSqlType(exp));
      return visitUnsupported(exp);
    }

    @Override
    public Pair<Object, SqlType> visitStructExpression(
        final CreateStructExpression node,
        final Void context
    ) {
//      final String schemaName = structToCodeName.apply(node);
//      final StringBuilder struct = new StringBuilder("new Struct(").append(schemaName).append(")");
//      for (final Field field : node.getFields()) {
//        struct.append(".put(")
//            .append('"')
//            .append(field.getName())
//            .append('"')
//            .append(",")
//            .append(process(field.getValue(), context).getLeft())
//            .append(")");
//      }
//      return new Pair<>(
//          "((Struct)" + struct.toString() + ")",
//          expressionTypeManager.getExpressionSqlType(node)
//      );
      return visitUnsupported(node);
    }

    @Override
    public Pair<Object, SqlType> visitBetweenPredicate(
        final BetweenPredicate node,
        final Void context
    ) {
//      final Pair<String, SqlType> compareMin = process(
//          new ComparisonExpression(
//              ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
//              node.getValue(),
//              node.getMin()),
//          context);
//      final Pair<String, SqlType> compareMax = process(
//          new ComparisonExpression(
//              ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
//              node.getValue(),
//              node.getMax()),
//          context);
//
//      // note that the entire expression must be surrounded by parentheses
//      // otherwise negations and other higher level operations will not work
//      return new Pair<>(
//          "(" + compareMin.getLeft() + " && " + compareMax.getLeft() + ")",
//          SqlTypes.BOOLEAN
//      );
      return visitUnsupported(node);
    }

    private String formatBinaryExpression(
        final String operator, final Expression left, final Expression right, final Void context
    ) {
      return "(" + process(left, context).getLeft() + " " + operator + " "
          + process(right, context).getLeft() + ")";
    }

    private String genCastCode(
        final Pair<String, SqlType> exp,
        final SqlType sqlType
    ) {
      return CastEvaluator.generateCode(exp.left, exp.right, sqlType, ksqlConfig);
    }
  }
}
