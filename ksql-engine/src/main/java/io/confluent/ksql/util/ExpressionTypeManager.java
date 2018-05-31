/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.codegen.FunctionArguments;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
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
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.planner.PlanException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Optional;

public class ExpressionTypeManager
    extends DefaultAstVisitor<Expression, ExpressionTypeManager.ExpressionTypeContext> {

  private final Schema schema;
  private final FunctionRegistry functionRegistry;
  private final FunctionArguments functionArguments = new FunctionArguments();

  public ExpressionTypeManager(Schema schema, final FunctionRegistry functionRegistry) {
    this.schema = schema;
    this.functionRegistry = functionRegistry;
  }

  public Schema getExpressionType(final Expression expression) {
    ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
    process(expression, expressionTypeContext);
    return expressionTypeContext.getSchema();
  }

  static class ExpressionTypeContext {

    Schema schema;

    public Schema getSchema() {
      return schema;
    }

    public void setSchema(Schema schema) {
      this.schema = schema;
    }
  }

  @Override
  protected Expression visitArithmeticBinary(final ArithmeticBinaryExpression node,
                                             final ExpressionTypeContext expressionTypeContext) {
    final int argCount = functionArguments.numCurrentFunctionArguments();
    process(node.getLeft(), expressionTypeContext);
    Schema leftType = expressionTypeContext.getSchema();
    process(node.getRight(), expressionTypeContext);
    Schema rightType = expressionTypeContext.getSchema();
    expressionTypeContext.setSchema(resolveArithmaticType(leftType, rightType));
    if (functionArguments.numCurrentFunctionArguments() > argCount + 1) {
      functionArguments.mergeTwoArguments(argCount);
    }
    return null;
  }

  protected Expression visitCast(final Cast node,
                                 final ExpressionTypeContext expressionTypeContext) {

    Schema castType = SchemaUtil.getTypeSchema(node.getType());
    updateContextAndFunctionArgs(castType, expressionTypeContext);
    return null;
  }

  @Override
  protected Expression visitComparisonExpression(
      final ComparisonExpression node, final ExpressionTypeContext expressionTypeContext) {

    updateContextAndFunctionArgs(Schema.BOOLEAN_SCHEMA, expressionTypeContext);
    return null;
  }

  @Override
  protected Expression visitQualifiedNameReference(
      final QualifiedNameReference node, final ExpressionTypeContext expressionTypeContext) {
    Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, node.getName().getSuffix());
    if (!schemaField.isPresent()) {
      throw new KsqlException(String.format("Invalid Expression %s.", node.toString()));
    }
    final Schema schema = schemaField.get().schema();
    updateContextAndFunctionArgs(schema, expressionTypeContext);
    return null;
  }

  @Override
  protected Expression visitDereferenceExpression(
      final DereferenceExpression node, final ExpressionTypeContext expressionTypeContext) {
    Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, node.toString());
    if (!schemaField.isPresent()) {
      throw new KsqlException(String.format("Invalid Expression %s.", node.toString()));
    }
    final Schema schema = schemaField.get().schema();
    updateContextAndFunctionArgs(schema, expressionTypeContext);
    return null;
  }

  protected Expression visitStringLiteral(final StringLiteral node,
                                          final ExpressionTypeContext expressionTypeContext) {
    updateContextAndFunctionArgs(Schema.STRING_SCHEMA, expressionTypeContext);
    return null;
  }

  protected Expression visitBooleanLiteral(final BooleanLiteral node,
                                           final ExpressionTypeContext expressionTypeContext) {
    updateContextAndFunctionArgs(Schema.BOOLEAN_SCHEMA, expressionTypeContext);
    return null;
  }

  protected Expression visitLongLiteral(final LongLiteral node,
                                        final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.INT64_SCHEMA);
    updateContextAndFunctionArgs(Schema.INT64_SCHEMA, expressionTypeContext);
    return null;
  }

  @Override
  protected Expression visitIntegerLiteral(final IntegerLiteral node,
                                           final ExpressionTypeContext context) {
    updateContextAndFunctionArgs(Schema.INT32_SCHEMA, context);
    return null;
  }

  protected Expression visitDoubleLiteral(final DoubleLiteral node,
                                          final ExpressionTypeContext expressionTypeContext) {
    updateContextAndFunctionArgs(Schema.FLOAT64_SCHEMA, expressionTypeContext);
    return null;
  }

  protected Expression visitLikePredicate(LikePredicate node,
                                          ExpressionTypeContext expressionTypeContext) {
    updateContextAndFunctionArgs(Schema.BOOLEAN_SCHEMA, expressionTypeContext);
    return null;
  }

  protected Expression visitIsNotNullPredicate(IsNotNullPredicate node,
                                               ExpressionTypeContext expressionTypeContext) {
    updateContextAndFunctionArgs(Schema.BOOLEAN_SCHEMA, expressionTypeContext);
    return null;
  }

  protected Expression visitIsNullPredicate(IsNullPredicate node,
                                            ExpressionTypeContext expressionTypeContext) {
    updateContextAndFunctionArgs(Schema.BOOLEAN_SCHEMA, expressionTypeContext);
    return null;
  }

  protected Expression visitSubscriptExpression(
      final SubscriptExpression node, final ExpressionTypeContext expressionTypeContext) {
    String arrayBaseName = node.getBase().toString();
    Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, arrayBaseName);
    if (!schemaField.isPresent()) {
      throw new KsqlException(String.format("Invalid Expression %s.", node.toString()));
    }
    final Schema schema = schemaField.get().schema().valueSchema();
    updateContextAndFunctionArgs(schema, expressionTypeContext);
    return null;
  }

  protected Expression visitFunctionCall(final FunctionCall node,
                                         final ExpressionTypeContext expressionTypeContext) {

    final UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName().getSuffix());
    if (udfFactory != null) {
      functionArguments.beginFunction();
      for (final Expression expression : node.getArguments()) {
        process(expression, expressionTypeContext);
      }
      final Schema returnType = udfFactory.getFunction(functionArguments.endFunction())
          .getReturnType();
      updateContextAndFunctionArgs(returnType, expressionTypeContext);
    } else if (functionRegistry.isAggregate(node.getName().getSuffix())) {
      KsqlAggregateFunction ksqlAggregateFunction =
          functionRegistry.getAggregate(
              node.getName().getSuffix(), getExpressionType(node.getArguments().get(0)));
      expressionTypeContext.setSchema(ksqlAggregateFunction.getReturnType());
    } else {
      throw new KsqlException("Unknown function: " + node.getName().toString());
    }
    return null;
  }

  private Schema resolveArithmaticType(final Schema leftSchema,
                                            final Schema rightSchema) {
    Schema.Type leftType = leftSchema.type();
    Schema.Type rightType = rightSchema.type();

    if (leftType == rightType) {
      return leftSchema;
    } else if (((leftType == Schema.Type.STRING) || (rightType == Schema.Type.STRING))
        || ((leftType == Schema.Type.BOOLEAN) || (rightType == Schema.Type.BOOLEAN))) {
      throw new PlanException("Incompatible types.");
    } else if ((leftType == Schema.Type.FLOAT64) || (rightType == Schema.Type.FLOAT64)) {
      return Schema.FLOAT64_SCHEMA;
    } else if ((leftType == Schema.Type.INT64) || (rightType == Schema.Type.INT64)) {
      return Schema.INT64_SCHEMA;
    } else if ((leftType == Schema.Type.INT32) || (rightType == Schema.Type.INT32)) {
      return Schema.INT32_SCHEMA;
    }
    throw new PlanException("Unsupported types.");
  }

  private Object updateContextAndFunctionArgs(final Schema schema,
                                              final ExpressionTypeContext context) {
    functionArguments.addArgumentType(schema.type());
    context.setSchema(schema);
    return null;
  }
}
