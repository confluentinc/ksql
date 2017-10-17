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

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlAggregateFunction;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.parser.tree.DefaultAstVisitor;
import io.confluent.ksql.parser.tree.*;
import io.confluent.ksql.planner.PlanException;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.Optional;

public class ExpressionTypeManager
    extends DefaultAstVisitor<Expression, ExpressionTypeManager.ExpressionTypeContext> {

  private final Schema schema;
  private final FunctionRegistry functionRegistry;

  public ExpressionTypeManager(Schema schema, final FunctionRegistry functionRegistry) {
    this.schema = schema;
    this.functionRegistry = functionRegistry;
  }

  public Schema getExpressionType(final Expression expression) {
    ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
    process(expression, expressionTypeContext);
    return expressionTypeContext.getSchema();
  }

  class ExpressionTypeContext {

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
    process(node.getLeft(), expressionTypeContext);
    Schema leftType = expressionTypeContext.getSchema();
    process(node.getRight(), expressionTypeContext);
    Schema rightType = expressionTypeContext.getSchema();
    expressionTypeContext.setSchema(resolveArithmaticType(leftType, rightType));
    return null;
  }

  protected Expression visitCast(final Cast node,
                                 final ExpressionTypeContext expressionTypeContext) {

    Schema castType = SchemaUtil.getTypeSchema(node.getType());
    expressionTypeContext.setSchema(castType);

    return null;
  }

  @Override
  protected Expression visitComparisonExpression(
      final ComparisonExpression node, final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitQualifiedNameReference(
      final QualifiedNameReference node, final ExpressionTypeContext expressionTypeContext) {
    Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, node.getName().getSuffix());
    if (!schemaField.isPresent()) {
      throw new KsqlException(String.format("Invalid Expression %s.", node.toString()));
    }
    expressionTypeContext.setSchema(schemaField.get().schema());
    return null;
  }

  @Override
  protected Expression visitDereferenceExpression(
      final DereferenceExpression node, final ExpressionTypeContext expressionTypeContext) {
    Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, node.toString());
    if (!schemaField.isPresent()) {
      throw new KsqlException(String.format("Invalid Expression %s.", node.toString()));
    }
    expressionTypeContext.setSchema(schemaField.get().schema());
    return null;
  }

  protected Expression visitStringLiteral(final StringLiteral node,
                                          final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.STRING_SCHEMA);
    return null;
  }

  protected Expression visitBooleanLiteral(final BooleanLiteral node,
                                           final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitLongLiteral(final LongLiteral node,
                                        final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.INT64_SCHEMA);
    return null;
  }

  protected Expression visitDoubleLiteral(final DoubleLiteral node,
                                          final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.FLOAT64_SCHEMA);
    return null;
  }

  protected Expression visitLikePredicate(LikePredicate node,
                                          ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitIsNotNullPredicate(IsNotNullPredicate node,
                                               ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitIsNullPredicate(IsNullPredicate node,
                                            ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitSubscriptExpression(
      final SubscriptExpression node, final ExpressionTypeContext expressionTypeContext) {
    String arrayBaseName = node.getBase().toString();
    Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, arrayBaseName);
    if (!schemaField.isPresent()) {
      throw new KsqlException(String.format("Invalid Expression %s.", node.toString()));
    }
    expressionTypeContext.setSchema(schemaField.get().schema().valueSchema());
    return null;
  }

  protected Expression visitFunctionCall(final FunctionCall node,
                                         final ExpressionTypeContext expressionTypeContext) {

    KsqlFunction ksqlFunction = functionRegistry.getFunction(node.getName().getSuffix());
    if (ksqlFunction != null) {
      expressionTypeContext.setSchema(ksqlFunction.getReturnType());
    } else if (functionRegistry.isAnAggregateFunction(node.getName().getSuffix())) {
      KsqlAggregateFunction ksqlAggregateFunction =
          functionRegistry.getAggregateFunction(
              node.getName().getSuffix(), node.getArguments(), schema);
      expressionTypeContext.setSchema(ksqlAggregateFunction.getReturnType());
    } else {
      throw new KsqlException("Unknown function: " + node.getName().toString());
    }
    return null;
  }

  private Schema resolveArithmaticType(final Schema leftSchema,
                                            final Schema rightSchema) {
    if (leftSchema == rightSchema) {
      return leftSchema;
    } else if ((leftSchema == Schema.STRING_SCHEMA) || (rightSchema == Schema.STRING_SCHEMA)) {
      throw new PlanException("Incompatible types.");
    } else if ((leftSchema == Schema.BOOLEAN_SCHEMA) || (rightSchema == Schema.BOOLEAN_SCHEMA)) {
      throw new PlanException("Incompatible types.");
    } else if ((leftSchema == Schema.FLOAT64_SCHEMA) || (rightSchema == Schema.FLOAT64_SCHEMA)) {
      return Schema.FLOAT64_SCHEMA;
    } else if ((leftSchema == Schema.INT64_SCHEMA) || (rightSchema == Schema.INT64_SCHEMA)) {
      return Schema.INT64_SCHEMA;
    } else if ((leftSchema == Schema.INT32_SCHEMA) || (rightSchema == Schema.INT32_SCHEMA)) {
      return Schema.INT32_SCHEMA;
    }
    throw new PlanException("Unsupported types.");
  }
}
