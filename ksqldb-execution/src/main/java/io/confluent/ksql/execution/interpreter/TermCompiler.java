package io.confluent.ksql.execution.interpreter;

import static io.confluent.ksql.execution.interpreter.ComparisonInterpreter.doCompareTo;
import static io.confluent.ksql.execution.interpreter.ComparisonInterpreter.doComparisonCheck;
import static io.confluent.ksql.execution.interpreter.ComparisonInterpreter.doEquals;
import static io.confluent.ksql.execution.interpreter.ComparisonInterpreter.doEqualsCheck;
import static io.confluent.ksql.execution.interpreter.ComparisonInterpreter.doNullCheck;
import static io.confluent.ksql.schema.ksql.SchemaConverters.sqlToFunctionConverter;
import static java.lang.String.format;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.ksql.execution.codegen.helpers.ArrayAccess;
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
import io.confluent.ksql.execution.interpreter.terms.ArithmeticUnaryTerm;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms;
import io.confluent.ksql.execution.interpreter.terms.BasicTerms.BooleanTerm;
import io.confluent.ksql.execution.interpreter.terms.ColumnReferenceTerm;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.ComparisonFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.ComparisonNullCheckFunction;
import io.confluent.ksql.execution.interpreter.terms.ComparisonTerm.EqualsFunction;
import io.confluent.ksql.execution.interpreter.terms.CreateArrayTerm;
import io.confluent.ksql.execution.interpreter.terms.CreateMapTerm;
import io.confluent.ksql.execution.interpreter.terms.DereferenceTerm;
import io.confluent.ksql.execution.interpreter.terms.FunctionCallTerm;
import io.confluent.ksql.execution.interpreter.terms.IsNotNullTerm;
import io.confluent.ksql.execution.interpreter.terms.IsNullTerm;
import io.confluent.ksql.execution.interpreter.terms.LikeTerm;
import io.confluent.ksql.execution.interpreter.terms.LogicalBinaryTerms;
import io.confluent.ksql.execution.interpreter.terms.NotTerm;
import io.confluent.ksql.execution.interpreter.terms.SearchedCaseTerm;
import io.confluent.ksql.execution.interpreter.terms.StructTerm;
import io.confluent.ksql.execution.interpreter.terms.SubscriptTerm;
import io.confluent.ksql.execution.interpreter.terms.Term;
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
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlArgument;
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
import java.util.Optional;
import java.util.stream.Collectors;

public class TermCompiler implements ExpressionVisitor<Pair<Term, SqlType>, Void> {

  private final FunctionRegistry functionRegistry;
  private final LogicalSchema schema;
  private final KsqlConfig ksqlConfig;
  private final ExpressionTypeManager expressionTypeManager;

  public TermCompiler(
      final FunctionRegistry functionRegistry,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final ExpressionTypeManager expressionTypeManager
  ) {
    this.functionRegistry = functionRegistry;
    this.schema = schema;
    this.ksqlConfig = ksqlConfig;
    this.expressionTypeManager = expressionTypeManager;
  }

  private Pair<Term, SqlType> visitIllegalState(final Expression expression) {
    throw new IllegalStateException(
        format("expression type %s should never be visited", expression.getClass()));
  }

  private Pair<Term, SqlType> visitUnsupported(final Expression expression) {
    throw new UnsupportedOperationException(
        format(
            "not yet implemented: %s.visit%s",
            getClass().getName(),
            expression.getClass().getSimpleName()
        )
    );
  }

  @Override
  public Pair<Term, SqlType> visitType(final Type node, final Void context) {
    return visitIllegalState(node);
  }

  @Override
  public Pair<Term, SqlType> visitWhenClause(final WhenClause whenClause, final Void context) {
    return visitIllegalState(whenClause);
  }

  @Override
  public Pair<Term, SqlType> visitInPredicate(
      final InPredicate inPredicate,
      final Void context
  ) {
    return visitUnsupported(inPredicate);
  }

  @Override
  public Pair<Term, SqlType> visitInListExpression(
      final InListExpression inListExpression, final Void context
  ) {
    return visitUnsupported(inListExpression);
  }

  @Override
  public Pair<Term, SqlType> visitTimestampLiteral(
      final TimestampLiteral node, final Void context
  ) {
    return new Pair<>(BasicTerms.of(node.getValue()), SqlTypes.TIMESTAMP);
  }

  @Override
  public Pair<Term, SqlType> visitTimeLiteral(
      final TimeLiteral timeLiteral,
      final Void context
  ) {
    return visitUnsupported(timeLiteral);
  }

  @Override
  public Pair<Term, SqlType> visitSimpleCaseExpression(
      final SimpleCaseExpression simpleCaseExpression, final Void context
  ) {
    return visitUnsupported(simpleCaseExpression);
  }

  @Override
  public Pair<Term, SqlType> visitBooleanLiteral(
      final BooleanLiteral node,
      final Void context
  ) {
    return new Pair<>(BasicTerms.of(node.getValue()), SqlTypes.BOOLEAN);
  }

  @Override
  public Pair<Term, SqlType> visitStringLiteral(final StringLiteral node, final Void context) {
    return new Pair<>(BasicTerms.of(node.getValue()), SqlTypes.STRING);
  }

  @Override
  public Pair<Term, SqlType> visitDoubleLiteral(final DoubleLiteral node, final Void context) {
    return new Pair<>(BasicTerms.of(node.getValue()), SqlTypes.DOUBLE);
  }

  @Override
  public Pair<Term, SqlType> visitDecimalLiteral(
      final DecimalLiteral decimalLiteral,
      final Void context
  ) {
    final SqlType sqlType = DecimalUtil.fromValue(decimalLiteral.getValue());
    return new Pair<>(
        BasicTerms.of(decimalLiteral.getValue(), sqlType),
        sqlType
    );
  }

  @Override
  public Pair<Term, SqlType> visitNullLiteral(final NullLiteral node, final Void context) {
    return new Pair<>(BasicTerms.ofNull(), null);
  }

  @Override
  public Pair<Term, SqlType> visitLambdaExpression(
      final LambdaFunctionCall lambdaFunctionCall, final Void context) {
    return visitUnsupported(lambdaFunctionCall);
  }

  @Override
  public Pair<Term, SqlType> visitLambdaVariable(
      final LambdaVariable lambdaLiteral, final Void context) {
    return visitUnsupported(lambdaLiteral);
  }

  @Override
  public Pair<Term, SqlType> visitUnqualifiedColumnReference(
      final UnqualifiedColumnReferenceExp node,
      final Void context
  ) {
    final Column schemaColumn = schema.findValueColumn(node.getColumnName())
        .orElseThrow(() ->
            new KsqlException("Field not found: " + node.getColumnName()));

    return new Pair<>(ColumnReferenceTerm.create(
        schemaColumn.index(), schemaColumn.type()), schemaColumn.type());
  }

  @Override
  public Pair<Term, SqlType> visitQualifiedColumnReference(
      final QualifiedColumnReferenceExp node,
      final Void context
  ) {
    throw new UnsupportedOperationException(
        "Qualified column reference must be resolved to unqualified reference before codegen"
    );
  }

  @Override
  public Pair<Term, SqlType> visitDereferenceExpression(
      final DereferenceExpression node, final Void context
  ) {
    final SqlType functionReturnSchema = expressionTypeManager.getExpressionSqlType(node);

    final Pair<Term, SqlType> base = process(node.getBase(), context);

    if (base.getRight().baseType() != SqlBaseType.STRUCT) {
      throw new KsqlException("Can only dereference Struct type, instead got " + base.getRight());
    }

    final Term struct = base.getLeft();
    return new Pair<>(new DereferenceTerm(struct, node.getFieldName(), functionReturnSchema),
        functionReturnSchema);
  }

  @Override
  public Pair<Term, SqlType> visitLongLiteral(final LongLiteral node, final Void context) {
    return new Pair<>(BasicTerms.of(node.getValue()), SqlTypes.BIGINT);
  }

  @Override
  public Pair<Term, SqlType> visitIntegerLiteral(
      final IntegerLiteral node,
      final Void context
  ) {
    return new Pair<>(BasicTerms.of(node.getValue()), SqlTypes.INTEGER);
  }

  @Override
  public Pair<Term, SqlType> visitFunctionCall(final FunctionCall node, final Void context) {
    final UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName());
    final List<SqlArgument> argumentSchemas = node.getArguments().stream()
        .map(expressionTypeManager::getExpressionSqlType)
        .map(SqlArgument::of)
        .collect(Collectors.toList());

    final KsqlScalarFunction function = udfFactory.getFunction(argumentSchemas);

    final SqlType functionReturnSchema = function.getReturnType(argumentSchemas);
    final Class<?> javaClass =
        SchemaConverters.sqlToJavaConverter().toJavaType(functionReturnSchema);

    final List<Expression> arguments = node.getArguments();

    final List<Term> args = new ArrayList<>();
    for (int i = 0; i < arguments.size(); i++) {
      final Expression arg = arguments.get(i);
      final SqlType sqlType = argumentSchemas.get(i).getSqlType();

      final ParamType paramType;
      if (i >= function.parameters().size() - 1 && function.isVariadic()) {
        paramType = ((ArrayType) Iterables.getLast(function.parameters())).element();
      } else {
        paramType = function.parameters().get(i);
      }

      // This will attempt to cast to the expected argument type and will throw an error if
      // it cannot be done.
      final Term argTerm = process(convertArgument(arg, sqlType, paramType), context).getLeft();
      args.add(argTerm);
    }

    final Kudf kudf = function.newInstance(ksqlConfig);
    return new Pair<>(new FunctionCallTerm(kudf, args, javaClass, functionReturnSchema),
        functionReturnSchema);
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
  public Pair<Term, SqlType> visitLogicalBinaryExpression(
      final LogicalBinaryExpression node, final Void context
  ) {
    final Pair<Term, SqlType> left = process(node.getLeft(), context);
    final Pair<Term, SqlType> right = process(node.getRight(), context);
    if (!(left.getRight().baseType() == SqlBaseType.BOOLEAN
        && right.getRight().baseType() == SqlBaseType.BOOLEAN)) {
      throw new KsqlException(
          format("Logical binary expects two boolean values.  Actual %s and %s",
              left.getRight(), right.getRight()));
    }

    final BooleanTerm leftBoolean = (BooleanTerm) left.getLeft();
    final BooleanTerm rightBoolean = (BooleanTerm) right.getLeft();

    return new Pair<>(
        LogicalBinaryTerms.create(node.getType(), leftBoolean, rightBoolean),
        SqlTypes.BOOLEAN
    );
  }

  @Override
  public Pair<Term, SqlType> visitNotExpression(final NotExpression node, final Void context) {
    final Pair<Term, SqlType> pair = process(node.getValue(), context);
    if (!(pair.getRight().baseType() == SqlBaseType.BOOLEAN)) {
      throw new IllegalStateException(
          format("Not expression expects a boolean value.  Actual %s", pair.getRight()));
    }

    final BooleanTerm exprBoolean = (BooleanTerm) pair.getLeft();
    return new Pair<>(new NotTerm(exprBoolean), SqlTypes.BOOLEAN);
  }

  @Override
  public Pair<Term, SqlType> visitComparisonExpression(
      final ComparisonExpression node, final Void context
  ) {
    final Pair<Term, SqlType> left = process(node.getLeft(), context);
    final Pair<Term, SqlType> right = process(node.getRight(), context);
    final Term leftTerm = left.getLeft();
    final Term rightTerm = right.getLeft();

    final ComparisonNullCheckFunction comparisonNullCheckFunction = doNullCheck(node.getType());

    final Optional<ComparisonFunction> compareTo = doCompareTo(left, right);
    if (compareTo.isPresent()) {
      return new Pair<>(doComparisonCheck(
          node.getType(), leftTerm, rightTerm, comparisonNullCheckFunction,
          compareTo.get()), SqlTypes.BOOLEAN);
    }

    final Optional<EqualsFunction> equals = doEquals(left, right);
    if (equals.isPresent()) {
      return new Pair<>(doEqualsCheck(
          node.getType(), leftTerm, rightTerm, comparisonNullCheckFunction, equals.get()),
          SqlTypes.BOOLEAN);
    }

    throw new KsqlException("Unsupported comparison between " + left.getRight() + " and "
        + right.getRight());
  }

  @Override
  public Pair<Term, SqlType> visitCast(final Cast node, final Void context) {
    final Pair<Term, SqlType> expr = process(node.getExpression(), context);
    final SqlType from = expr.right;
    final SqlType to = node.getType().getSqlType();
    return new Pair<>(
        CastInterpreter.cast(expr.left, from, to, ksqlConfig),
        to);
  }

  @Override
  public Pair<Term, SqlType> visitIsNullPredicate(
      final IsNullPredicate node,
      final Void context
  ) {
    final Pair<Term, SqlType> value = process(node.getValue(), context);
    return new Pair<>(new IsNullTerm(value.getLeft()), SqlTypes.BOOLEAN);
  }

  @Override
  public Pair<Term, SqlType> visitIsNotNullPredicate(
      final IsNotNullPredicate node,
      final Void context
  ) {
    final Pair<Term, SqlType> value = process(node.getValue(), context);
    return new Pair<>(new IsNotNullTerm(value.getLeft()), SqlTypes.BOOLEAN);
  }

  @Override
  public Pair<Term, SqlType> visitArithmeticUnary(
      final ArithmeticUnaryExpression node, final Void context
  ) {
    final Pair<Term, SqlType> value = process(node.getValue(), context);
    switch (node.getSign()) {
      case MINUS:
        return visitArithmeticMinus(value);
      case PLUS:
        return visitArithmeticPlus(value);
      default:
        throw new UnsupportedOperationException("Unsupported sign: " + node.getSign());
    }
  }

  private Pair<Term, SqlType> visitArithmeticMinus(final Pair<Term, SqlType> value) {
    if (value.getRight().baseType()  == SqlBaseType.DECIMAL) {
      return new Pair<>(
          new ArithmeticUnaryTerm(value.getLeft(),
              o -> ((BigDecimal) o).negate(new MathContext(((SqlDecimal) value.getRight()).getPrecision(),
                  RoundingMode.UNNECESSARY))),
          value.getRight()
      );
    } else if (value.getRight().baseType() == SqlBaseType.DOUBLE) {
      return new Pair<>(
          new ArithmeticUnaryTerm(value.getLeft(), o -> -((Double) o)), value.getRight());
    } else if (value.getRight().baseType() == SqlBaseType.INTEGER) {
      return new Pair<>(
          new ArithmeticUnaryTerm(value.getLeft(), o -> -((Integer) o)), value.getRight());
    } else if (value.getRight().baseType() == SqlBaseType.BIGINT) {
      return new Pair<>(
          new ArithmeticUnaryTerm(value.getLeft(), o -> -((Long) o)), value.getRight());
    } else {
      throw new UnsupportedOperationException("Negation on unsupported type: "
          + value.getRight());
    }
  }

  private Pair<Term, SqlType> visitArithmeticPlus(final Pair<Term, SqlType> value) {
    if (value.getRight().baseType() == SqlBaseType.DECIMAL) {
      return new Pair<>(
          new ArithmeticUnaryTerm(value.getLeft(),
              o -> ((BigDecimal) o)
                  .plus(new MathContext(((SqlDecimal) value.getRight()).getPrecision(),
                      RoundingMode.UNNECESSARY))),
          value.getRight()
      );
    } else if (value.getRight().baseType() == SqlBaseType.DOUBLE) {
      return new Pair<>(
          new ArithmeticUnaryTerm(value.getLeft(), o -> +((Double) o)), value.getRight());
    } else if (value.getRight().baseType() == SqlBaseType.INTEGER) {
      return new Pair<>(
          new ArithmeticUnaryTerm(value.getLeft(), o -> +((Integer) o)), value.getRight());
    } else if (value.getRight().baseType() == SqlBaseType.BIGINT) {
      return new Pair<>(
          new ArithmeticUnaryTerm(value.getLeft(), o -> +((Long) o)), value.getRight());
    } else {
      throw new UnsupportedOperationException("Unary plus on unsupported type: "
          + value.getLeft());
    }
  }

  @Override
  public Pair<Term, SqlType> visitArithmeticBinary(
      final ArithmeticBinaryExpression node, final Void context
  ) {
    final Pair<Term, SqlType> left = process(node.getLeft(), context);
    final Pair<Term, SqlType> right = process(node.getRight(), context);

    final SqlType schema = expressionTypeManager.getExpressionSqlType(node);

    final Term term = ArithmeticInterpreter.doArithmetic(
        node, left, right, schema, ksqlConfig);
    return new Pair<>(term, schema);
  }

  @Override
  public Pair<Term, SqlType> visitSearchedCaseExpression(
      final SearchedCaseExpression node, final Void context
  ) {
    final SqlType resultSchema = expressionTypeManager.getExpressionSqlType(node);

    final List<Pair<Term, Term>> operandResultTerms = node
        .getWhenClauses()
        .stream()
        .map(whenClause ->
            Pair.of(process(whenClause.getOperand(), context).getLeft(),
                process(whenClause.getResult(), context).getLeft()))
        .collect(ImmutableList.toImmutableList());

    final Optional<Term> defaultValueTerm = node.getDefaultValue()
        .map(exp -> process(node.getDefaultValue().get(), context).getLeft());

    return new Pair<>(new SearchedCaseTerm(operandResultTerms, defaultValueTerm, resultSchema),
        resultSchema);
  }

  @Override
  public Pair<Term, SqlType> visitLikePredicate(final LikePredicate node, final Void context) {
    final Term patternString = process(node.getPattern(), context).getLeft();
    final Term valueString = process(node.getValue(), context).getLeft();

    return new Pair<>(
        new LikeTerm(patternString, valueString, node.getEscape()),
        SqlTypes.STRING
    );
  }

  @Override
  public Pair<Term, SqlType> visitSubscriptExpression(
      final SubscriptExpression node,
      final Void context
  ) {
    final SqlType internalSchema = expressionTypeManager.getExpressionSqlType(node.getBase());
    switch (internalSchema.baseType()) {
      case ARRAY:
        final SqlArray array = (SqlArray) internalSchema;
        final Term listTerm = process(node.getBase(), context).getLeft();
        final Term indexTerm = process(node.getIndex(), context).getLeft();
        return new Pair<>(new SubscriptTerm(listTerm, indexTerm,
            (o, index) -> ArrayAccess.arrayAccess(((List<?>) o), (Integer) index),
            array.getItemType()),
            array.getItemType()
        );

      case MAP:
        final SqlMap mapSchema = (SqlMap) internalSchema;
        final Term mapTerm = process(node.getBase(), context).getLeft();
        final Term keyTerm = process(node.getIndex(), context).getLeft();
        return new Pair<>(new SubscriptTerm(mapTerm, keyTerm,
            (map, key) -> ((Map<?, ?>) map).get(key), mapSchema.getValueType()),
            mapSchema.getValueType()
        );
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public Pair<Term, SqlType> visitCreateArrayExpression(
      final CreateArrayExpression exp,
      final Void context
  ) {
    final List<Expression> expressions = CoercionUtil
        .coerceUserList(exp.getValues(), expressionTypeManager)
        .expressions();

    List<Term> arrayTerms = expressions
        .stream()
        .map(value -> process(value, context).getLeft())
        .collect(ImmutableList.toImmutableList());

    SqlType sqlType = expressionTypeManager.getExpressionSqlType(exp);
    return new Pair<>(
        new CreateArrayTerm(arrayTerms, sqlType),
        sqlType);
  }

  @Override
  public Pair<Term, SqlType> visitCreateMapExpression(
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

    final Iterable<Pair<Expression, Expression>> pairs = () -> Streams.zip(
        keys.stream(), values.stream(), Pair::of)
        .iterator();
    ImmutableMap.Builder<Term, Term> mapTerms = ImmutableMap.builder();
    for (Pair<Expression, Expression> p : pairs) {
      mapTerms.put(
          process(p.getLeft(), context).getLeft(),
          process(p.getRight(), context).getLeft());
    }

    final SqlType resultType = expressionTypeManager.getExpressionSqlType(exp);
    return new Pair<>(
        new CreateMapTerm(mapTerms.build(), resultType),
        resultType);
  }

  @Override
  public Pair<Term, SqlType> visitStructExpression(
      final CreateStructExpression node,
      final Void context
  ) {

    ImmutableMap.Builder<String, Term> nameToTerm = ImmutableMap.builder();
    for (final Field field : node.getFields()) {
      nameToTerm.put(field.getName(), process(field.getValue(), context).getLeft());
    }
    final SqlType resultType = expressionTypeManager.getExpressionSqlType(node);
    return new Pair<>(
        new StructTerm(nameToTerm.build(), resultType), resultType
    );
  }

  @Override
  public Pair<Term, SqlType> visitBetweenPredicate(
      final BetweenPredicate node,
      final Void context
  ) {
    Expression and = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
        new ComparisonExpression(
            ComparisonExpression.Type.GREATER_THAN_OR_EQUAL,
            node.getValue(),
            node.getMin()),
        new ComparisonExpression(
            ComparisonExpression.Type.LESS_THAN_OR_EQUAL,
            node.getValue(),
            node.getMax()));
    return process(and, context);
  }
}
