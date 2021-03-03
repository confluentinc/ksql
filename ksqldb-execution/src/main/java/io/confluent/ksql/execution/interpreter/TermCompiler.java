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

package io.confluent.ksql.execution.interpreter;

import static io.confluent.ksql.schema.ksql.SchemaConverters.sqlToFunctionConverter;
import static java.lang.String.format;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Streams;
import io.confluent.ksql.execution.codegen.helpers.ArrayAccess;
import io.confluent.ksql.execution.codegen.helpers.InListEvaluator;
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
import io.confluent.ksql.execution.interpreter.terms.ColumnReferenceTerm;
import io.confluent.ksql.execution.interpreter.terms.CreateArrayTerm;
import io.confluent.ksql.execution.interpreter.terms.CreateMapTerm;
import io.confluent.ksql.execution.interpreter.terms.DereferenceTerm;
import io.confluent.ksql.execution.interpreter.terms.FunctionCallTerm;
import io.confluent.ksql.execution.interpreter.terms.InPredicateTerm;
import io.confluent.ksql.execution.interpreter.terms.IsNotNullTerm;
import io.confluent.ksql.execution.interpreter.terms.IsNullTerm;
import io.confluent.ksql.execution.interpreter.terms.LikeTerm;
import io.confluent.ksql.execution.interpreter.terms.LiteralTerms;
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
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.DecimalUtil;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.Pair;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

// CHECKSTYLE_RULES.OFF: ClassDataAbstractionCoupling
public class TermCompiler implements ExpressionVisitor<Term, Void> {
  // CHECKSTYLE_RULES.ON: ClassDataAbstractionCoupling

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

  private Term visitIllegalState(final Expression expression) {
    throw new IllegalStateException(
        format("Expression type %s should never be visited.%n"
                + "Check if there's an existing issue: "
                + "https://github.com/confluentinc/ksql/issues %n"
                + "If not, please file a new one with your expression.",
            expression.getClass()));
  }

  private Term visitUnsupported(final Expression expression) {
    throw new UnsupportedOperationException(
        format("Not yet implemented: %s.visit%s.%n"
                + "Check if there's an existing issue: "
                + "https://github.com/confluentinc/ksql/issues %n"
                + "If not, please file a new one with your expression.",
            getClass().getName(),
            expression.getClass().getSimpleName()
        )
    );
  }

  @Override
  public Term visitType(final Type node, final Void context) {
    return visitIllegalState(node);
  }

  @Override
  public Term visitWhenClause(final WhenClause whenClause, final Void context) {
    return visitIllegalState(whenClause);
  }

  @Override
  public Term visitInPredicate(
      final InPredicate inPredicate,
      final Void context
  ) {
    final InPredicate preprocessed = InListEvaluator
        .preprocess(inPredicate, expressionTypeManager);

    final Term value = process(preprocessed.getValue(), context);

    final List<Term> valueList = preprocessed.getValueList().getValues().stream()
        .map(v -> process(v, context))
        .collect(ImmutableList.toImmutableList());

    return new InPredicateTerm(value, valueList);
  }

  @Override
  public Term visitInListExpression(
      final InListExpression inListExpression, final Void context
  ) {
    return visitUnsupported(inListExpression);
  }

  @Override
  public Term visitTimestampLiteral(
      final TimestampLiteral node, final Void context
  ) {
    return LiteralTerms.of(node.getValue());
  }

  @Override
  public Term visitTimeLiteral(
      final TimeLiteral timeLiteral,
      final Void context
  ) {
    return visitUnsupported(timeLiteral);
  }

  @Override
  public Term visitSimpleCaseExpression(
      final SimpleCaseExpression simpleCaseExpression, final Void context
  ) {
    return visitUnsupported(simpleCaseExpression);
  }

  @Override
  public Term visitBooleanLiteral(
      final BooleanLiteral node,
      final Void context
  ) {
    return LiteralTerms.of(node.getValue());
  }

  @Override
  public Term visitStringLiteral(final StringLiteral node, final Void context) {
    return LiteralTerms.of(node.getValue());
  }

  @Override
  public Term visitDoubleLiteral(final DoubleLiteral node, final Void context) {
    return LiteralTerms.of(node.getValue());
  }

  @Override
  public Term visitDecimalLiteral(
      final DecimalLiteral decimalLiteral,
      final Void context
  ) {
    final SqlType sqlType = DecimalUtil.fromValue(decimalLiteral.getValue());
    return LiteralTerms.of(decimalLiteral.getValue(), sqlType);
  }

  @Override
  public Term visitNullLiteral(final NullLiteral node, final Void context) {
    return LiteralTerms.ofNull();
  }

  @Override
  public Term visitLambdaExpression(
      final LambdaFunctionCall lambdaFunctionCall, final Void context) {
    return visitUnsupported(lambdaFunctionCall);
  }

  @Override
  public Term visitLambdaVariable(
      final LambdaVariable lambdaLiteral, final Void context) {
    return visitUnsupported(lambdaLiteral);
  }

  @Override
  public Term visitUnqualifiedColumnReference(
      final UnqualifiedColumnReferenceExp node,
      final Void context
  ) {
    final Column schemaColumn = schema.findValueColumn(node.getColumnName())
        .orElseThrow(() ->
            new KsqlException("Field not found: " + node.getColumnName()));

    return new ColumnReferenceTerm(schemaColumn.index(), schemaColumn.type());
  }

  @Override
  public Term visitQualifiedColumnReference(
      final QualifiedColumnReferenceExp node,
      final Void context
  ) {
    throw new IllegalStateException(
        "Qualified column reference must be resolved to unqualified reference before interpreter"
    );
  }

  @Override
  public Term visitDereferenceExpression(
      final DereferenceExpression node, final Void context
  ) {
    final SqlType functionReturnSchema = expressionTypeManager.getExpressionSqlType(node);

    final Term struct = process(node.getBase(), context);

    if (struct.getSqlType().baseType() != SqlBaseType.STRUCT) {
      throw new KsqlException("Can only dereference Struct type, instead got "
          + struct.getSqlType());
    }

    return new DereferenceTerm(struct, node.getFieldName(), functionReturnSchema);
  }

  @Override
  public Term visitLongLiteral(final LongLiteral node, final Void context) {
    return LiteralTerms.of(node.getValue());
  }

  @Override
  public Term visitIntegerLiteral(
      final IntegerLiteral node,
      final Void context
  ) {
    return LiteralTerms.of(node.getValue());
  }

  @Override
  public Term visitFunctionCall(final FunctionCall node, final Void context) {
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
      // lambda arguments and null values are considered to have null type
      final SqlType sqlType = argumentSchemas.get(i).getSqlType().orElse(null);;

      final ParamType paramType;
      if (i >= function.parameters().size() - 1 && function.isVariadic()) {
        paramType = ((ArrayType) Iterables.getLast(function.parameters())).element();
      } else {
        paramType = function.parameters().get(i);
      }

      // This will attempt to cast to the expected argument type and will throw an error if
      // it cannot be done.
      final Term argTerm = process(convertArgument(arg, sqlType, paramType), context);
      args.add(argTerm);
    }

    final Kudf kudf = function.newInstance(ksqlConfig);
    return new FunctionCallTerm(kudf, args, javaClass, functionReturnSchema);
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
  public Term visitLogicalBinaryExpression(
      final LogicalBinaryExpression node, final Void context
  ) {
    final Term left = process(node.getLeft(), context);
    final Term right = process(node.getRight(), context);
    if (!(left.getSqlType().baseType() == SqlBaseType.BOOLEAN
        && right.getSqlType().baseType() == SqlBaseType.BOOLEAN)) {
      throw new KsqlException(
          format("Logical binary expects two boolean values.  Actual %s and %s",
              left.getSqlType(), right.getSqlType()));
    }

    return LogicalBinaryTerms.create(node.getType(), left, right);
  }

  @Override
  public Term visitNotExpression(final NotExpression node, final Void context) {
    final Term term = process(node.getValue(), context);
    if (!(term.getSqlType().baseType() == SqlBaseType.BOOLEAN)) {
      throw new IllegalStateException(
          format("Not expression expects a boolean value.  Actual %s", term.getSqlType()));
    }

    return new NotTerm(term);
  }

  @Override
  public Term visitComparisonExpression(
      final ComparisonExpression node, final Void context
  ) {
    final Term left = process(node.getLeft(), context);
    final Term right = process(node.getRight(), context);

    return ComparisonInterpreter.doComparison(node.getType(), left, right);
  }

  @Override
  public Term visitCast(final Cast node, final Void context) {
    final Term term = process(node.getExpression(), context);
    final SqlType from = term.getSqlType();
    final SqlType to = node.getType().getSqlType();
    return CastInterpreter.cast(term, from, to, ksqlConfig);
  }

  @Override
  public Term visitIsNullPredicate(
      final IsNullPredicate node,
      final Void context
  ) {
    final Term value = process(node.getValue(), context);
    return new IsNullTerm(value);
  }

  @Override
  public Term visitIsNotNullPredicate(
      final IsNotNullPredicate node,
      final Void context
  ) {
    final Term value = process(node.getValue(), context);
    return new IsNotNullTerm(value);
  }

  @Override
  public Term visitArithmeticUnary(
      final ArithmeticUnaryExpression node, final Void context
  ) {
    final Term value = process(node.getValue(), context);
    return ArithmeticInterpreter.doUnaryArithmetic(node.getSign(), value);
  }

  @Override
  public Term visitArithmeticBinary(
      final ArithmeticBinaryExpression node, final Void context
  ) {
    final Term left = process(node.getLeft(), context);
    final Term right = process(node.getRight(), context);

    final SqlType schema = expressionTypeManager.getExpressionSqlType(node);

    return ArithmeticInterpreter.doBinaryArithmetic(node.getOperator(), left, right, schema,
        ksqlConfig);
  }

  @Override
  public Term visitSearchedCaseExpression(
      final SearchedCaseExpression node, final Void context
  ) {
    final SqlType resultSchema = expressionTypeManager.getExpressionSqlType(node);

    final List<Pair<Term, Term>> operandResultTerms = node
        .getWhenClauses()
        .stream()
        .map(whenClause ->
            Pair.of(process(whenClause.getOperand(), context),
                process(whenClause.getResult(), context)))
        .collect(ImmutableList.toImmutableList());

    final Optional<Term> defaultValueTerm = node.getDefaultValue()
        .map(exp -> process(node.getDefaultValue().get(), context));

    return new SearchedCaseTerm(operandResultTerms, defaultValueTerm, resultSchema);
  }

  @Override
  public Term visitLikePredicate(final LikePredicate node, final Void context) {
    final Term patternString = process(node.getPattern(), context);
    final Term valueString = process(node.getValue(), context);

    return new LikeTerm(patternString, valueString, node.getEscape());
  }

  @Override
  public Term visitSubscriptExpression(
      final SubscriptExpression node,
      final Void context
  ) {
    final SqlType internalSchema = expressionTypeManager.getExpressionSqlType(node.getBase());
    switch (internalSchema.baseType()) {
      case ARRAY:
        final SqlArray array = (SqlArray) internalSchema;
        final Term listTerm = process(node.getBase(), context);
        final Term indexTerm = process(node.getIndex(), context);
        return new SubscriptTerm(listTerm, indexTerm,
            (o, index) -> ArrayAccess.arrayAccess(((List<?>) o), (Integer) index),
            array.getItemType());

      case MAP:
        final SqlMap mapSchema = (SqlMap) internalSchema;
        final Term mapTerm = process(node.getBase(), context);
        final Term keyTerm = process(node.getIndex(), context);
        return new SubscriptTerm(mapTerm, keyTerm,
            (map, key) -> ((Map<?, ?>) map).get(key), mapSchema.getValueType());
      default:
        throw new UnsupportedOperationException();
    }
  }

  @Override
  public Term visitCreateArrayExpression(
      final CreateArrayExpression exp,
      final Void context
  ) {
    final List<Expression> expressions = CoercionUtil
        .coerceUserList(exp.getValues(), expressionTypeManager)
        .expressions();

    final List<Term> arrayTerms = expressions
        .stream()
        .map(value -> process(value, context))
        .collect(ImmutableList.toImmutableList());

    final SqlType sqlType = expressionTypeManager.getExpressionSqlType(exp);
    return new CreateArrayTerm(arrayTerms, sqlType);
  }

  @Override
  public Term visitCreateMapExpression(
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
    final ImmutableMap.Builder<Term, Term> mapTerms = ImmutableMap.builder();
    for (Pair<Expression, Expression> p : pairs) {
      mapTerms.put(
          process(p.getLeft(), context),
          process(p.getRight(), context));
    }

    final SqlType resultType = expressionTypeManager.getExpressionSqlType(exp);
    return new CreateMapTerm(mapTerms.build(), resultType);
  }

  @Override
  public Term visitStructExpression(
      final CreateStructExpression node,
      final Void context
  ) {

    final ImmutableMap.Builder<String, Term> nameToTerm = ImmutableMap.builder();
    for (final Field field : node.getFields()) {
      nameToTerm.put(field.getName(), process(field.getValue(), context));
    }
    final SqlType resultType = expressionTypeManager.getExpressionSqlType(node);
    return new StructTerm(nameToTerm.build(), resultType);
  }

  @Override
  public Term visitBetweenPredicate(
      final BetweenPredicate node,
      final Void context
  ) {
    final Expression and = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND,
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
