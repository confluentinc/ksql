/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.util;

import com.google.common.collect.ImmutableMap;
import io.confluent.ksql.function.KSQLAggregateFunction;
import io.confluent.ksql.function.KSQLFunction;
import io.confluent.ksql.function.KSQLFunctions;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.BooleanLiteral;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DefaultASTVisitor;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
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

public class ExpressionTypeManager
    extends DefaultASTVisitor<Expression, ExpressionTypeManager.ExpressionTypeContext> {

  final Schema schema;
  ImmutableMap<String, Schema> schemaImmutableMap;
  SchemaUtil schemaUtil = new SchemaUtil();

  public ExpressionTypeManager(Schema schema) {
    this.schema = schema;
  }

  public ExpressionTypeManager(final Schema schema,
                               final ImmutableMap<String, Schema> schemaImmutableMap) {
    this.schema = schema;
    this.schemaImmutableMap = schemaImmutableMap;
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
  protected Expression visitComparisonExpression(final ComparisonExpression node,
                                                 final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  @Override
  protected Expression visitQualifiedNameReference(final QualifiedNameReference node,
                                                   final ExpressionTypeContext expressionTypeContext) {
    Field schemaField = SchemaUtil.getFieldByName(schema, node.getName().getSuffix());
    expressionTypeContext.setSchema(schemaField.schema());
    return null;
  }

  @Override
  protected Expression visitDereferenceExpression(final DereferenceExpression node,
                                                  final ExpressionTypeContext expressionTypeContext) {
    Field schemaField = SchemaUtil.getFieldByName(schema, node.toString());
    expressionTypeContext.setSchema(schemaField.schema());
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

  protected Expression visitLikePredicate(LikePredicate node, ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitIsNotNullPredicate(IsNotNullPredicate node, ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitIsNullPredicate(IsNullPredicate node, ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setSchema(Schema.BOOLEAN_SCHEMA);
    return null;
  }

  protected Expression visitSubscriptExpression(final SubscriptExpression node, final ExpressionTypeContext expressionTypeContext) {
    String arrayBaseName = node.getBase().toString();
    Field schemaField = SchemaUtil.getFieldByName(schema, arrayBaseName);
    expressionTypeContext.setSchema(schemaField.schema().valueSchema());
    return null;
  }

  protected Expression visitFunctionCall(final FunctionCall node,
                                         final ExpressionTypeContext expressionTypeContext) {

    KSQLFunction ksqlFunction = KSQLFunctions.getFunction(node.getName().getSuffix());
    if (ksqlFunction != null) {
      expressionTypeContext.setSchema(ksqlFunction.getReturnType());
    } else if (KSQLFunctions.isAnAggregateFunction(node.getName().getSuffix())) {
      KSQLAggregateFunction ksqlAggregateFunction = KSQLFunctions.getAggregateFunction(node.getName().getSuffix(), node.getArguments(), schema);
      expressionTypeContext.setSchema(ksqlAggregateFunction.getReturnType());
    } else {
      throw new KSQLException("Unknown function: " + node.getName().toString());
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
