/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.util;

import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.KsqlFunctions;
import io.confluent.ksql.function.udf.Kudf;
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
import io.confluent.ksql.parser.tree.SubscriptExpression;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.util.HashMap;
import java.util.Map;

public class ExpressionUtil {

  public Map<String, Class> getParameterInfo(final Expression expression, final Schema schema) {
    Visitor visitor = new Visitor(schema);
    visitor.process(expression, null);
    return visitor.parameterMap;
  }

  public ExpressionMetadata getExpressionEvaluator(
      final Expression expression,
      final Schema schema) throws Exception {
    ExpressionUtil expressionUtil = new ExpressionUtil();
    Map<String, Class> parameterMap = expressionUtil.getParameterInfo(expression, schema);

    String[] parameterNames = new String[parameterMap.size()];
    Class[] parameterTypes = new Class[parameterMap.size()];
    int[] columnIndexes = new int[parameterMap.size()];
    Kudf[] kudfObjects = new Kudf[parameterMap.size()];

    int index = 0;
    for (String parameterName : parameterMap.keySet()) {
      parameterNames[index] = parameterName;
      parameterTypes[index] = parameterMap.get(parameterName);
      columnIndexes[index] = SchemaUtil.getFieldIndexByName(schema, parameterName);
      if (columnIndexes[index] < 0) {
        kudfObjects[index] = (Kudf) parameterMap.get(parameterName).newInstance();
      } else {
        kudfObjects[index] = null;
      }
      index++;
    }

    String expressionStr = expression.getCodegenString(schema);
    IExpressionEvaluator
        ee =
        CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

    // The expression will have two "int" parameters: "a" and "b".
    ee.setParameters(parameterNames, parameterTypes);

    // And the expression (i.e. "result") type is also "int".
    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(schema);
    Schema expressionType = expressionTypeManager.getExpressionType(expression);

    ee.setExpressionType(SchemaUtil.getJavaType(expressionType));

    // And now we "cook" (scan, parse, compile and load) the fabulous expression.
    ee.cook(expressionStr);

    return new ExpressionMetadata(ee, columnIndexes, kudfObjects, expressionType);
  }

  private class Visitor
      extends AstVisitor<Object, Object> {

    final Schema schema;
    final Map<String, Class> parameterMap;

    Visitor(Schema schema) {
      this.schema = schema;
      this.parameterMap = new HashMap<>();
    }

    protected Object visitLikePredicate(LikePredicate node, Object context) {
      process(node.getValue(), null);
      return null;
    }

    protected Object visitFunctionCall(FunctionCall node, Object context) {
      String functionName = node.getName().getSuffix();
      KsqlFunction ksqlFunction = KsqlFunctions.getFunction(functionName);
      parameterMap.put(node.getName().getSuffix(),
//                       SchemaUtil.getJavaType(ksqlFunction.getReturnType()));
                       ksqlFunction.getKudfClass());
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
      Field schemaField = SchemaUtil.getFieldByName(schema, node.toString());
      if (schemaField == null) {
        throw new RuntimeException(
            "Cannot find the select field in the available fields: " + node.toString());
      }
      parameterMap.put(schemaField.name().replace(".", "_"),
                       SchemaUtil.getJavaType(schemaField.schema()));
      return null;
    }

    @Override
    protected Object visitCast(Cast node, Object context) {

      process(node.getExpression(), context);
      return null;
    }

    @Override
    protected Object visitSubscriptExpression(SubscriptExpression node, Object context) {
      String arrayBaseName = node.getBase().toString();
      Field schemaField = SchemaUtil.getFieldByName(schema, arrayBaseName);
      if (schemaField == null) {
        throw new RuntimeException(
            "Cannot find the select field in the available fields: " + arrayBaseName);
      }
      parameterMap.put(schemaField.name().replace(".", "_"),
                       SchemaUtil.getJavaType(schemaField.schema()));
      process(node.getIndex(), context);
      return null;
    }

    @Override
    protected Object visitQualifiedNameReference(QualifiedNameReference node, Object context) {
      Field schemaField = SchemaUtil.getFieldByName(schema, node.getName().getSuffix());
      if (schemaField == null) {
        throw new RuntimeException(
            "Cannot find the select field in the available fields: " + node.getName().getSuffix());
      }
      parameterMap.put(schemaField.name().replace(".", "_"),
                       SchemaUtil.getJavaType(schemaField.schema()));
      return null;
    }
  }

}
