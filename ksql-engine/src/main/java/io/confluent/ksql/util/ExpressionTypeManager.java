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

package io.confluent.ksql.util;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.structfieldextractor.FetchFieldFromStruct;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.BetweenPredicate;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DefaultAstVisitor;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.IntegerLiteral;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.WhenClause;
import io.confluent.ksql.schema.Operator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class ExpressionTypeManager
    extends DefaultAstVisitor<Expression, ExpressionTypeManager.ExpressionTypeContext> {

  private final LogicalSchema schema;
  private final FunctionRegistry functionRegistry;

  public ExpressionTypeManager(
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  public Schema getExpressionSchema(final Expression expression) {
    final ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
    process(expression, expressionTypeContext);
    return expressionTypeContext.getSchema();
  }

  static class ExpressionTypeContext {

    private Schema schema;

    public Schema getSchema() {
      return schema;
    }

    public void setSchema(final Schema schema) {
      this.schema = schema;
    }
  }

  @Override
  protected Expression visitArithmeticBinary(final ArithmeticBinaryExpression node,
      final ExpressionTypeContext expressionTypeContext) {
    process(node.getLeft(), expressionTypeContext);
    final Schema leftType = expressionTypeContext.getSchema();
    process(node.getRight(), expressionTypeContext);
    final Schema rightType = expressionTypeContext.getSchema();
    expressionTypeContext.setSchema(resolveArithmeticType(leftType, rightType, node.getOperator()));
    return null;
  }

  @Override
  protected Expression visitNotExpression(
      final NotExpression node, final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitCast(
      final Cast node,
      final ExpressionTypeContext expressionTypeContext
  ) {
    final SqlType sqlType = node.getType().getSqlType();
    if (!sqlType.supportsCast()) {
      throw new KsqlFunctionException("Only casts to primitive types or decimals "
          + "are supported: " + sqlType);
    }

    final Schema castType = SchemaConverters
        .sqlToLogicalConverter()
        .fromSqlType(sqlType);

    expressionTypeContext.setSchema(castType);
    return null;
  }

  @Override
  protected Expression visitComparisonExpression(
      final ComparisonExpression node, final ExpressionTypeContext expressionTypeContext) {
    process(node.getLeft(), expressionTypeContext);
    final Schema leftSchema = expressionTypeContext.getSchema();
    process(node.getRight(), expressionTypeContext);
    final Schema rightSchema = expressionTypeContext.getSchema();
    ComparisonUtil.isValidComparison(leftSchema, node.getType(), rightSchema);
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitBetweenPredicate(final BetweenPredicate node,
      final ExpressionTypeContext context) {
    context.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitQualifiedNameReference(
      final QualifiedNameReference node,
      final ExpressionTypeContext expressionTypeContext
  ) {
    final Field schemaField = schema.findValueField(node.getName().getSuffix())
        .orElseThrow(() ->
            new KsqlException(String.format("Invalid Expression %s.", node.toString())));

    final Schema qualifiedNameReferenceSchema = schemaField.schema();
    expressionTypeContext.setSchema(qualifiedNameReferenceSchema);
    return null;
  }

  @Override
  protected Expression visitDereferenceExpression(
      final DereferenceExpression node,
      final ExpressionTypeContext expressionTypeContext
  ) {
    final Field schemaField = schema.findValueField(node.toString())
        .orElseThrow(() ->
            new KsqlException(String.format("Invalid Expression %s.", node.toString())));

    final Schema dereferenceExpressionSchema = schemaField.schema();
    expressionTypeContext.setSchema(dereferenceExpressionSchema);
    return null;
  }

  @Override
  protected Expression visitStringLiteral(final StringLiteral node,
      final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_STRING_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitBooleanLiteral(final BooleanLiteral node,
      final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitLongLiteral(final LongLiteral node,
      final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_INT64_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitIntegerLiteral(final IntegerLiteral node,
      final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_INT32_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitDoubleLiteral(final DoubleLiteral node,
      final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_FLOAT64_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitNullLiteral(final NullLiteral node,
      final ExpressionTypeContext context) {
    context.setSchema(null);
    return null;
  }

  @Override
  protected Expression visitLikePredicate(final LikePredicate node,
      final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitIsNotNullPredicate(final IsNotNullPredicate node,
      final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitIsNullPredicate(final IsNullPredicate node,
      final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitSearchedCaseExpression(
      final SearchedCaseExpression node,
      final ExpressionTypeContext expressionTypeContext) {
    validateSearchedCaseExpression(node);
    process(node.getWhenClauses().get(0).getResult(), expressionTypeContext);
    return null;
  }

  @Override
  protected Expression visitSubscriptExpression(
      final SubscriptExpression node,
      final ExpressionTypeContext expressionTypeContext
  ) {
    process(node.getBase(), expressionTypeContext);
    final Schema arrayMapSchema = expressionTypeContext.getSchema();
    expressionTypeContext.setSchema(arrayMapSchema.valueSchema());
    return null;
  }

  @Override
  protected Expression visitFunctionCall(
      final FunctionCall node,
      final ExpressionTypeContext expressionTypeContext) {

    if (functionRegistry.isAggregate(node.getName().getSuffix())) {
      final Schema schema = node.getArguments().isEmpty()
          ? FunctionRegistry.DEFAULT_FUNCTION_ARG_SCHEMA
          : getExpressionSchema(node.getArguments().get(0));

      final KsqlAggregateFunction aggFunc = functionRegistry
          .getAggregate(node.getName().getSuffix(), schema);

      expressionTypeContext.setSchema(aggFunc.getReturnType());
      return null;
    }
    if (node.getName().getSuffix().equalsIgnoreCase(FetchFieldFromStruct.FUNCTION_NAME)) {
      process(node.getArguments().get(0), expressionTypeContext);
      final Schema firstArgSchema = expressionTypeContext.getSchema();
      final String fieldName = ((StringLiteral) node.getArguments().get(1)).getValue();
      if (firstArgSchema.field(fieldName) == null) {
        throw new KsqlException(String.format("Could not find field %s in %s.",
            fieldName,
            node.getArguments().get(0).toString()));
      }
      final Schema returnSchema = firstArgSchema.field(fieldName).schema();
      expressionTypeContext.setSchema(returnSchema);
    } else {
      final UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName().getSuffix());
      final List<Schema> argTypes = new ArrayList<>();
      for (final Expression expression : node.getArguments()) {
        process(expression, expressionTypeContext);
        argTypes.add(expressionTypeContext.getSchema());
      }
      final Schema returnType = udfFactory.getFunction(argTypes)
          .getReturnType();
      expressionTypeContext.setSchema(returnType);
    }
    return null;
  }

  private static Schema resolveArithmeticType(
      final Schema leftSchema,
      final Schema rightSchema,
      final Operator operator) {
    return SchemaUtil.resolveBinaryOperatorResultType(leftSchema, rightSchema, operator);
  }

  private void validateSearchedCaseExpression(final SearchedCaseExpression searchedCaseExpression) {
    final Schema firstResultSchema = getExpressionSchema(
        searchedCaseExpression.getWhenClauses().get(0).getResult());
    searchedCaseExpression.getWhenClauses()
        .forEach(whenClause -> validateWhenClause(whenClause, firstResultSchema));
    searchedCaseExpression.getDefaultValue()
        .map(this::getExpressionSchema)
        .filter(defaultSchema -> !firstResultSchema.equals(defaultSchema))
        .ifPresent(badSchema -> {
          throw new KsqlException("Invalid Case expression."
              + " Schema for the default clause should be the same as schema for THEN clauses."
              + " Result scheme: " + firstResultSchema + "."
              + " Schema for default expression is " + badSchema);
        });
  }

  private void validateWhenClause(final WhenClause whenClause, final Schema expectedResultSchema) {
    final Schema operandSchema = getExpressionSchema(whenClause.getOperand());
    if (!operandSchema.equals(Schema.OPTIONAL_BOOLEAN_SCHEMA)) {
      throw new KsqlException("When operand schema should be boolean. Schema for ("
          + whenClause.getOperand() + ") is " + operandSchema);
    }
    final Schema resultSchema = getExpressionSchema(whenClause.getResult());
    if (!expectedResultSchema.equals(resultSchema)) {
      throw new KsqlException("Invalid Case expression."
          + " Schemas for 'THEN' clauses should be the same."
          + " Result schema: " + expectedResultSchema + "."
          + " Schema for THEN expression '" + whenClause + "'"
          + " is " + resultSchema);
    }
  }
}
