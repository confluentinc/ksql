/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.analyzer;

import io.confluent.ksql.function.KQLFunction;
import io.confluent.ksql.function.KQLFunctions;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.util.SchemaUtil;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;


public class ExpressionAnalyzer {
  final Schema schema;
  final boolean isJoinSchema;

  public ExpressionAnalyzer(Schema schema, boolean isJoinSchema) {
    this.schema = schema;
    this.isJoinSchema = isJoinSchema;
  }

  public void analyzeExpression(Expression expression) {
    Visitor visitor = new Visitor(schema);
    visitor.process(expression, null);
  }

  private class Visitor
      extends AstVisitor<Object, Object> {

    final Schema schema;

    Visitor(Schema schema) {
      this.schema = schema;
    }

    protected Object visitLikePredicate(LikePredicate node, Object context) {
      process(node.getValue(), null);
      return null;
    }

    protected Object visitFunctionCall(FunctionCall node, Object context) {
      String functionName = node.getName().getSuffix();
      KQLFunction kqlFunction = KQLFunctions.getFunction(functionName);
      for (Expression argExpr : node.getArguments()) {
        process(argExpr, null);
      }
      return null;
    }

    protected Object visitArithmeticBinary(ArithmeticBinaryExpression node, Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    protected Object visitIsNotNullPredicate(IsNotNullPredicate node, Object context) {
      return process(node.getValue(), context);
    }

    protected Object visitIsNullPredicate(IsNullPredicate node, Object context) {
      return process(node.getValue(), context);
    }

    protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    protected Object visitComparisonExpression(ComparisonExpression node, Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    protected Object visitNotExpression(NotExpression node, Object context) {
      return process(node.getValue(), null);
    }

    @Override
    protected Object visitDereferenceExpression(DereferenceExpression node, Object context) {
      String columnName = node.getFieldName();
      if (isJoinSchema) {
        columnName = node.toString();
      }
      Field schemaField = SchemaUtil.getFieldByName(schema, columnName);
      if (schemaField == null) {
        throw new RuntimeException(
            String.format("Column %s cannot be resolved.", columnName));
      }
      return null;
    }

    @Override
    protected Object visitCast(Cast node, Object context) {

      process(node.getExpression(), context);
      return null;
    }

    @Override
    protected Object visitQualifiedNameReference(QualifiedNameReference node, Object context) {
      String columnName = node.getName().getSuffix();
      columnName = columnName.substring(columnName.indexOf(".") + 1);
      Field schemaField = SchemaUtil.getFieldByName(schema, columnName);
      if (schemaField == null) {
        throw new RuntimeException(
            String.format("Column %s cannot be resolved.", columnName));
      }
      return null;
    }
  }

}
