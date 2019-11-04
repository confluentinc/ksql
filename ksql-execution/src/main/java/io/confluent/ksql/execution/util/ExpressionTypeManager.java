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

package io.confluent.ksql.execution.util;

import com.google.common.collect.ImmutableList;
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
import io.confluent.ksql.execution.function.UdafUtil;
import io.confluent.ksql.function.AggregateFunctionInitArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlFunctionException;
import io.confluent.ksql.function.KsqlTableFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.UdfMetadata;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SqlBaseType;
import io.confluent.ksql.schema.ksql.types.Field;
import io.confluent.ksql.schema.ksql.types.SqlArray;
import io.confluent.ksql.schema.ksql.types.SqlMap;
import io.confluent.ksql.schema.ksql.types.SqlStruct;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.VisitorUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.kafka.connect.data.Schema;

@SuppressWarnings("deprecation") // Need to migrate away from Connect Schema use.
public class ExpressionTypeManager {

  private final LogicalSchema schema;
  private final FunctionRegistry functionRegistry;

  public ExpressionTypeManager(
      final LogicalSchema schema,
      final FunctionRegistry functionRegistry
  ) {
    this.schema = Objects.requireNonNull(schema, "schema");
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
  }

  /**
   * @deprecated use getExpressionSqlType in new code.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @Deprecated
  public Schema getExpressionSchema(final Expression expression) {
    final ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
    new Visitor().process(expression, expressionTypeContext);
    return expressionTypeContext.getSchema();
  }

  public SqlType getExpressionSqlType(final Expression expression) {
    final ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
    new Visitor().process(expression, expressionTypeContext);
    return expressionTypeContext.getSqlType();
  }

  private static class ExpressionTypeContext {

    private SqlType sqlType;

    SqlType getSqlType() {
      return sqlType;
    }

    Schema getSchema() {
      return sqlType == null
          ? null
          : SchemaConverters.sqlToConnectConverter().toConnectSchema(sqlType);
    }

    void setSqlType(final SqlType sqlType) {
      this.sqlType = sqlType;
    }

    void setSchema(final Schema schema) {
      this.sqlType = SchemaConverters.connectToSqlConverter().toSqlType(schema);
    }
  }

  private class Visitor implements ExpressionVisitor<Void, ExpressionTypeContext> {

    @Override
    public Void visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      process(node.getLeft(), expressionTypeContext);
      final SqlType leftType = expressionTypeContext.getSqlType();

      process(node.getRight(), expressionTypeContext);
      final SqlType rightType = expressionTypeContext.getSqlType();

      final SqlType resultType = node.getOperator().resultType(leftType, rightType);

      expressionTypeContext.setSqlType(resultType);
      return null;
    }

    @Override
    public Void visitArithmeticUnary(
        final ArithmeticUnaryExpression node,
        final ExpressionTypeContext context
    ) {
      process(node.getValue(), context);
      return null;
    }

    @Override
    public Void visitNotExpression(
        final NotExpression node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitCast(
        final Cast node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      final SqlType sqlType = node.getType().getSqlType();
      if (!sqlType.supportsCast()) {
        throw new KsqlFunctionException("Only casts to primitive types or decimals "
            + "are supported: " + sqlType);
      }

      expressionTypeContext.setSqlType(sqlType);
      return null;
    }

    @Override
    public Void visitComparisonExpression(
        final ComparisonExpression node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      process(node.getLeft(), expressionTypeContext);
      final Schema leftSchema = expressionTypeContext.getSchema();
      process(node.getRight(), expressionTypeContext);
      final Schema rightSchema = expressionTypeContext.getSchema();
      ComparisonUtil.isValidComparison(leftSchema, node.getType(), rightSchema);
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitBetweenPredicate(
        final BetweenPredicate node,
        final ExpressionTypeContext context
    ) {
      context.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitColumnReference(
        final ColumnReferenceExp node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      final Column schemaColumn = schema.findColumn(node.getReference())
          .orElseThrow(() ->
              new KsqlException(String.format("Invalid Expression %s.", node.toString())));

      expressionTypeContext.setSqlType(schemaColumn.type());
      return null;
    }

    @Override
    public Void visitDereferenceExpression(
        final DereferenceExpression node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      process(node.getBase(), expressionTypeContext);
      final SqlType sqlType = expressionTypeContext.getSqlType();
      if (!(sqlType instanceof SqlStruct)) {
        throw new IllegalStateException("Expected STRUCT type, got: " + sqlType);
      }

      final SqlStruct structType = (SqlStruct)sqlType;
      final String fieldName = node.getFieldName();

      final Field structField = structType
          .field(fieldName)
          .orElseThrow(() -> new KsqlException(
              "Could not find field '" + fieldName + "' in '" + node.getBase() + "'.")
          );

      expressionTypeContext.setSqlType(structField.type());
      return null;
    }

    @Override
    public Void visitStringLiteral(
        final StringLiteral node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.STRING);
      return null;
    }

    @Override
    public Void visitBooleanLiteral(
        final BooleanLiteral node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitLongLiteral(
        final LongLiteral node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BIGINT);
      return null;
    }

    @Override
    public Void visitIntegerLiteral(
        final IntegerLiteral node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.INTEGER);
      return null;
    }

    @Override
    public Void visitDoubleLiteral(
        final DoubleLiteral node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.DOUBLE);
      return null;
    }

    @Override
    public Void visitNullLiteral(
        final NullLiteral node,
        final ExpressionTypeContext context
    ) {
      context.setSqlType(null);
      return null;
    }

    @Override
    public Void visitLikePredicate(
        final LikePredicate node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitIsNotNullPredicate(
        final IsNotNullPredicate node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitIsNullPredicate(
        final IsNullPredicate node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      expressionTypeContext.setSqlType(SqlTypes.BOOLEAN);
      return null;
    }

    @Override
    public Void visitSearchedCaseExpression(
        final SearchedCaseExpression node,
        final ExpressionTypeContext context
    ) {
      final Optional<SqlType> whenType = validateWhenClauses(node.getWhenClauses(), context);

      final Optional<SqlType> defaultType = node.getDefaultValue()
          .map(ExpressionTypeManager.this::getExpressionSqlType);

      if (whenType.isPresent() && defaultType.isPresent()) {
        if (!whenType.get().equals(defaultType.get())) {
          throw new KsqlException("Invalid Case expression. "
              + "Type for the default clause should be the same as for 'THEN' clauses."
              + System.lineSeparator()
              + "THEN type: " + whenType.get() + "."
              + System.lineSeparator()
              + "DEFAULT type: " + defaultType.get() + "."
          );
        }

        context.setSqlType(whenType.get());
      } else if (whenType.isPresent()) {
        context.setSqlType(whenType.get());
      } else if (defaultType.isPresent()) {
        context.setSqlType(defaultType.get());
      } else {
        throw new KsqlException("Invalid Case expression. All case branches have NULL type");
      }
      return null;
    }

    @Override
    public Void visitSubscriptExpression(
        final SubscriptExpression node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      process(node.getBase(), expressionTypeContext);
      final SqlType arrayMapType = expressionTypeContext.getSqlType();

      final SqlType valueType;
      if (arrayMapType instanceof SqlMap) {
        valueType = ((SqlMap)arrayMapType).getValueType();
      } else if (arrayMapType instanceof SqlArray) {
        valueType = ((SqlArray)arrayMapType).getItemType();
      } else {
        throw new UnsupportedOperationException("Unsupported container type: " + arrayMapType);
      }

      expressionTypeContext.setSqlType(valueType);
      return null;
    }

    @Override
    public Void visitFunctionCall(
        final FunctionCall node,
        final ExpressionTypeContext expressionTypeContext
    ) {
      if (functionRegistry.isAggregate(node.getName().name())) {
        final Schema schema = node.getArguments().isEmpty()
            ? FunctionRegistry.DEFAULT_FUNCTION_ARG_SCHEMA
            : getExpressionSchema(node.getArguments().get(0));

        final AggregateFunctionInitArguments args =
            UdafUtil.createAggregateFunctionInitArgs(0, node);

        final KsqlAggregateFunction aggFunc = functionRegistry
            .getAggregateFunction(node.getName().name(), schema, args);

        expressionTypeContext.setSchema(aggFunc.getReturnType());
        return null;
      }

      if (functionRegistry.isTableFunction(node.getName().name())) {
        final List<Schema> argumentTypes = node.getArguments().isEmpty()
            ? ImmutableList.of(FunctionRegistry.DEFAULT_FUNCTION_ARG_SCHEMA)
            : node.getArguments().stream().map(ExpressionTypeManager.this::getExpressionSchema)
                .collect(Collectors.toList());

        final KsqlTableFunction tableFunction = functionRegistry
            .getTableFunction(node.getName().name(), argumentTypes);

        expressionTypeContext.setSchema(tableFunction.getReturnType(argumentTypes));
        return null;
      }

      final UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName().name());
      final UdfMetadata metadata = udfFactory.getMetadata();
      if (metadata.isInternal()) {
        // Internal UDFs, e.g. FetchFieldFromStruct, should not be used directly by users:
        throw new KsqlException(
            "Can't find any functions with the name '" + node.getName().name() + "'");
      }

      final List<Schema> argTypes = new ArrayList<>();
      for (final Expression expression : node.getArguments()) {
        process(expression, expressionTypeContext);
        argTypes.add(expressionTypeContext.getSchema());
      }

      final Schema returnSchema = udfFactory.getFunction(argTypes).getReturnType(argTypes);
      expressionTypeContext.setSchema(returnSchema);
      return null;
    }

    @Override
    public Void visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final ExpressionTypeContext context) {
      process(node.getLeft(), context);
      process(node.getRight(), context);
      return null;
    }

    @Override
    public Void visitType(
        final Type type,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.illegalState(this, type);
    }

    @Override
    public Void visitTimeLiteral(
        final TimeLiteral timeLiteral,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, timeLiteral);
    }

    @Override
    public Void visitTimestampLiteral(
        final TimestampLiteral timestampLiteral,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, timestampLiteral);
    }

    @Override
    public Void visitDecimalLiteral(
        final DecimalLiteral decimalLiteral,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, decimalLiteral);
    }

    @Override
    public Void visitSimpleCaseExpression(
        final SimpleCaseExpression simpleCaseExpression,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, simpleCaseExpression);
    }

    @Override
    public Void visitInListExpression(
        final InListExpression inListExpression,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, inListExpression);
    }

    @Override
    public Void visitInPredicate(
        final InPredicate inPredicate,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.unsupportedOperation(this, inPredicate);
    }

    @Override
    public Void visitWhenClause(
        final WhenClause whenClause,
        final ExpressionTypeContext expressionTypeContext
    ) {
      throw VisitorUtil.illegalState(this, whenClause);
    }

    private Optional<SqlType> validateWhenClauses(
        final List<WhenClause> whenClauses,
        final ExpressionTypeContext context
    ) {
      Optional<SqlType> previousResult = Optional.empty();
      for (final WhenClause whenClause : whenClauses) {
        process(whenClause.getOperand(), context);

        final SqlType operandType = context.getSqlType();

        if (operandType.baseType() != SqlBaseType.BOOLEAN) {
          throw new KsqlException("WHEN operand type should be boolean."
              + System.lineSeparator()
              + "Type for '" + whenClause.getOperand() + "' is " + operandType
          );
        }

        process(whenClause.getResult(), context);

        final SqlType resultType = context.getSqlType();
        if (resultType == null) {
          continue; // `null` type
        }

        if (!previousResult.isPresent()) {
          previousResult = Optional.of(resultType);
          continue;
        }

        if (!previousResult.get().equals(resultType)) {
          throw new KsqlException("Invalid Case expression. "
              + "Type for all 'THEN' clauses should be the same."
              + System.lineSeparator()
              + "THEN expression '" + whenClause + "' has type: " + resultType + "."
              + System.lineSeparator()
              + "Previous THEN expression(s) type: " + previousResult.get() + ".");
        }
      }

      return previousResult;
    }
  }
}
