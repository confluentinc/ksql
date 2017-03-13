/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.util;

import com.google.common.collect.ImmutableMap;

import io.confluent.kql.function.KQLFunction;
import io.confluent.kql.function.KQLFunctions;
import io.confluent.kql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.kql.parser.tree.BooleanLiteral;
import io.confluent.kql.parser.tree.Cast;
import io.confluent.kql.parser.tree.ComparisonExpression;
import io.confluent.kql.parser.tree.DefaultASTVisitor;
import io.confluent.kql.parser.tree.DereferenceExpression;
import io.confluent.kql.parser.tree.DoubleLiteral;
import io.confluent.kql.parser.tree.Expression;
import io.confluent.kql.parser.tree.FunctionCall;
import io.confluent.kql.parser.tree.LongLiteral;
import io.confluent.kql.parser.tree.QualifiedNameReference;
import io.confluent.kql.parser.tree.StringLiteral;
import io.confluent.kql.planner.PlanException;

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

  public Schema.Type getExpressionType(final Expression expression) {
    ExpressionTypeContext expressionTypeContext = new ExpressionTypeContext();
    process(expression, expressionTypeContext);
    return expressionTypeContext.getType();
  }

  class ExpressionTypeContext {

    Schema.Type type;

    public Schema.Type getType() {
      return type;
    }

    public void setType(Schema.Type type) {
      this.type = type;
    }
  }

  @Override
  protected Expression visitArithmeticBinary(final ArithmeticBinaryExpression node,
                                             final ExpressionTypeContext expressionTypeContext) {
    process(node.getLeft(), expressionTypeContext);
    Schema.Type leftType = expressionTypeContext.getType();
    process(node.getRight(), expressionTypeContext);
    Schema.Type rightType = expressionTypeContext.getType();
    expressionTypeContext.setType(resolveArithmaticType(leftType, rightType));
    return null;
  }

  protected Expression visitCast(final Cast node,
                                 final ExpressionTypeContext expressionTypeContext) {

    Schema.Type castType = SchemaUtil.getTypeSchema(node.getType());
    expressionTypeContext.setType(castType);

    return null;
  }

  @Override
  protected Expression visitComparisonExpression(final ComparisonExpression node,
                                                 final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setType(Schema.Type.BOOLEAN);
    return null;
  }

  @Override
  protected Expression visitQualifiedNameReference(final QualifiedNameReference node,
                                                   final ExpressionTypeContext expressionTypeContext) {
    Field schemaField = SchemaUtil.getFieldByName(schema, node.getName().getSuffix());
    expressionTypeContext.setType(schemaField.schema().type());
    return null;
  }

  @Override
  protected Expression visitDereferenceExpression(final DereferenceExpression node,
                                                  final ExpressionTypeContext expressionTypeContext) {
//        Field schemaField = SchemaUtil.getFieldByName(schema, node.getFieldName());
    Field schemaField = SchemaUtil.getFieldByName(schema, node.toString());
    expressionTypeContext.setType(schemaField.schema().type());
    return null;
  }

  protected Expression visitStringLiteral(final StringLiteral node,
                                          final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setType(Schema.Type.STRING);
    return null;
  }

  protected Expression visitBooleanLiteral(final BooleanLiteral node,
                                           final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setType(Schema.Type.BOOLEAN);
    return null;
  }

  protected Expression visitLongLiteral(final LongLiteral node,
                                        final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setType(Schema.Type.INT64);
    return null;
  }

  protected Expression visitDoubleLiteral(final DoubleLiteral node,
                                          final ExpressionTypeContext expressionTypeContext) {
    expressionTypeContext.setType(Schema.Type.FLOAT64);
    return null;
  }

  protected Expression visitFunctionCall(final FunctionCall node,
                                         final ExpressionTypeContext expressionTypeContext) {
//    return visitExpression(node, expressionTypeContext);

    KQLFunction kqlFunction = KQLFunctions.getFunction(node.getName().getSuffix());
    KQLFunction kqlAggFunction = KQLFunctions.getAggregateFunction(node.getName().getSuffix());
    if (kqlFunction != null) {
      expressionTypeContext.setType(kqlFunction.getReturnType());
    } else if (kqlAggFunction != null) {
      expressionTypeContext.setType(kqlAggFunction.getReturnType());
    } else {
      throw new KQLException("Unknown function: " + node.getName().toString());
    }

    return null;
  }

  private Schema.Type resolveArithmaticType(final Schema.Type leftType,
                                            final Schema.Type rightType) {
    if (leftType == rightType) {
      return leftType;
    } else if ((leftType == Schema.Type.STRING) || (rightType == Schema.Type.STRING)) {
      throw new PlanException("Incompatible types.");
    } else if ((leftType == Schema.Type.BOOLEAN) || (rightType == Schema.Type.BOOLEAN)) {
      throw new PlanException("Incompatible types.");
    } else if ((leftType == Schema.Type.FLOAT64) || (rightType == Schema.Type.FLOAT64)) {
      return Schema.Type.FLOAT64;
    } else if ((leftType == Schema.Type.INT64) || (rightType == Schema.Type.INT64)) {
      return Schema.Type.INT64;
    } else if ((leftType == Schema.Type.INT32) || (rightType == Schema.Type.INT32)) {
      return Schema.Type.INT32;
    }

    throw new PlanException("Unsupported types.");
  }
}
