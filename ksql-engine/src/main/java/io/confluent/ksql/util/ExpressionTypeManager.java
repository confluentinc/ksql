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
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.structfieldextractor.FetchFieldFromStruct;
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
import io.confluent.ksql.parser.tree.NullLiteral;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

public class ExpressionTypeManager
    extends DefaultAstVisitor<Expression, ExpressionTypeManager.ExpressionTypeContext> {

  private final Schema schema;
  private final FunctionRegistry functionRegistry;

  public ExpressionTypeManager(final Schema schema, final FunctionRegistry functionRegistry) {
    this.schema = schema;
    this.functionRegistry = functionRegistry;
  }

  public Schema getExpressionSchema(final Expression expression) {
    final ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
    process(expression, expressionTypeContext);
    if (expressionTypeContext.getSchema() == null) {
      return null;
    }
    return expressionTypeContext.getSchema();
  }

  static class ExpressionTypeContext {

    Schema schema;

    public Schema getSchema() {
      return schema;
    }

    public Schema.Type getSchemaType() {
      if (schema == null) {
        return null;
      }
      return schema.type();
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
    expressionTypeContext.setSchema(resolveArithmeticType(leftType, rightType));
    return null;
  }

  protected Expression visitCast(final Cast node,
                                 final ExpressionTypeContext expressionTypeContext) {

    final Schema castType = SchemaUtil.getTypeSchema(node.getType());
    expressionTypeContext.setSchema(castType);
    return null;
  }

  @Override
  protected Expression visitComparisonExpression(
      final ComparisonExpression node, final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitQualifiedNameReference(
      final QualifiedNameReference node, final ExpressionTypeContext expressionTypeContext) {
    final Optional<Field> schemaField =
        SchemaUtil.getFieldByName(schema, node.getName().getSuffix());
    if (!schemaField.isPresent()) {
      throw new KsqlException(String.format("Invalid Expression %s.", node.toString()));
    }
    final Schema qualifiedNameReferenceSchema = schemaField.get().schema();
    expressionTypeContext.setSchema(qualifiedNameReferenceSchema);
    return null;
  }

  @Override
  protected Expression visitDereferenceExpression(
      final DereferenceExpression node, final ExpressionTypeContext expressionTypeContext) {
    final Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, node.toString());
    if (!schemaField.isPresent()) {
      throw new KsqlException(String.format("Invalid Expression %s.", node.toString()));
    }
    final Schema dereferenceExpressionSchema = schemaField.get().schema();
    expressionTypeContext.setSchema(dereferenceExpressionSchema);
    return null;
  }

  protected Expression visitStringLiteral(final StringLiteral node,
                                          final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_STRING_SCHEMA);
    return null;
  }

  protected Expression visitBooleanLiteral(final BooleanLiteral node,
                                           final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

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

  protected Expression visitLikePredicate(final LikePredicate node,
                                          final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitIsNotNullPredicate(final IsNotNullPredicate node,
                                               final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitIsNullPredicate(final IsNullPredicate node,
                                            final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.OPTIONAL_BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitSubscriptExpression(
      final SubscriptExpression node,
      final ExpressionTypeContext expressionTypeContext
  ) {
    process(node.getBase(), expressionTypeContext);
    final Schema arrayMapSchema = expressionTypeContext.getSchema();
    expressionTypeContext.setSchema(arrayMapSchema.valueSchema());
    return null;
  }

  protected Expression visitFunctionCall(
      final FunctionCall node,
      final ExpressionTypeContext expressionTypeContext) {

    if (functionRegistry.isAggregate(node.getName().getSuffix())) {
      final KsqlAggregateFunction ksqlAggregateFunction =
          functionRegistry.getAggregate(
              node.getName().getSuffix(), getExpressionSchema(node.getArguments().get(0)));
      expressionTypeContext.setSchema(ksqlAggregateFunction.getReturnType());
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

  private Schema resolveArithmeticType(final Schema leftSchema,
                                       final Schema rightSchema) {
    return SchemaUtil.resolveArithmeticType(leftSchema.type(), rightSchema.type());
  }
}
