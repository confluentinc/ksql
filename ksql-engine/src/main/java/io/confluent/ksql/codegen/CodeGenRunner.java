/*
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

package io.confluent.ksql.codegen;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.AstVisitor;
import io.confluent.ksql.parser.tree.Cast;
import io.confluent.ksql.parser.tree.ComparisonExpression;
import io.confluent.ksql.parser.tree.DereferenceExpression;
import io.confluent.ksql.parser.tree.DoubleLiteral;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.parser.tree.FunctionCall;
import io.confluent.ksql.parser.tree.IsNotNullPredicate;
import io.confluent.ksql.parser.tree.IsNullPredicate;
import io.confluent.ksql.parser.tree.LikePredicate;
import io.confluent.ksql.parser.tree.LogicalBinaryExpression;
import io.confluent.ksql.parser.tree.LongLiteral;
import io.confluent.ksql.parser.tree.NotExpression;
import io.confluent.ksql.parser.tree.QualifiedNameReference;
import io.confluent.ksql.parser.tree.StringLiteral;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.SchemaUtil;

public class CodeGenRunner {

  private final Schema schema;
  private final FunctionRegistry functionRegistry;

  public CodeGenRunner(Schema schema, FunctionRegistry functionRegistry) {
    this.functionRegistry = functionRegistry;
    this.schema = schema;
  }

  public Map<String, KsqlFunction> getParameterInfo(final Expression expression) {
    Visitor visitor = new Visitor(schema, functionRegistry);
    visitor.process(expression, null);
    return visitor.parameterMap;
  }

  public ExpressionMetadata buildCodeGenFromParseTree(
      final Expression expression
  ) throws Exception {
    Map<String, KsqlFunction> parameterMap = getParameterInfo(expression);

    String[] parameterNames = new String[parameterMap.size()];
    Class[] parameterTypes = new Class[parameterMap.size()];
    int[] columnIndexes = new int[parameterMap.size()];
    Kudf[] kudfObjects = new Kudf[parameterMap.size()];

    int index = 0;
    for (Map.Entry<String, KsqlFunction> entry : parameterMap.entrySet()) {
      parameterNames[index] = entry.getKey();
      parameterTypes[index] = entry.getValue().getKudfClass();
      columnIndexes[index] = SchemaUtil.getFieldIndexByName(schema, entry.getKey());
      if (columnIndexes[index] < 0) {
        kudfObjects[index] = entry.getValue().newInstance();
      } else {
        kudfObjects[index] = null;
      }
      index++;
    }

    String javaCode = new SqlToJavaVisitor(schema, functionRegistry).process(expression);

    IExpressionEvaluator ee =
        CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();

    ee.setParameters(parameterNames, parameterTypes);

    ExpressionTypeManager expressionTypeManager = new ExpressionTypeManager(
        schema,
        functionRegistry
    );
    Schema expressionType = expressionTypeManager.getExpressionType(expression);

    ee.setExpressionType(SchemaUtil.getJavaType(expressionType));

    ee.cook(javaCode);

    return new ExpressionMetadata(ee, columnIndexes, kudfObjects, expressionType);
  }

  private static class Visitor extends AstVisitor<Object, Object> {

    private final Schema schema;
    private final Map<String, KsqlFunction> parameterMap;
    private final FunctionRegistry functionRegistry;

    private final Stack<List<Schema.Type>> functionArgs = new Stack<>();
    private int functionCounter = 0;

    Visitor(Schema schema, FunctionRegistry functionRegistry) {
      this.schema = schema;
      this.parameterMap = new HashMap<>();
      this.functionRegistry = functionRegistry;
    }

    protected Object visitLikePredicate(LikePredicate node, Object context) {
      process(node.getValue(), null);
      return null;
    }

    protected Object visitFunctionCall(FunctionCall node, Object context) {
      final int functionNumber = functionCounter++;
      functionArgs.push(new ArrayList<>());
      final String functionName = node.getName().getSuffix();
      for (Expression argExpr : node.getArguments()) {
        process(argExpr, null);
      }

      final UdfFactory holder = functionRegistry.getUdfFactory(functionName);
      final KsqlFunction function = holder.getFunction(functionArgs.pop());
      parameterMap.put(
          node.getName().getSuffix() + "_" + functionNumber,
          function);
      if (inFunctionCall()) {
        functionArgs.peek().add(function.getReturnType().type());
      }
      return null;
    }

    private boolean inFunctionCall() {
      return !functionArgs.isEmpty();
    }

    protected Object visitArithmeticBinary(ArithmeticBinaryExpression node, Object context) {
      final List<Schema.Type> functionArgTypes = inFunctionCall()
          ? functionArgs.peek()
          : Collections.emptyList();
      final int index = functionArgTypes.size();
      process(node.getLeft(), null);
      process(node.getRight(), null);
      if (inFunctionCall() && functionArgTypes.size() > index + 1) {
        final Schema.Type first = functionArgTypes.remove(index);
        final Schema.Type second = functionArgTypes.remove(index);
        functionArgTypes.add(first.ordinal() < second.ordinal() ? second : first);
      }
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
      Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, node.toString());
      if (!schemaField.isPresent()) {
        throw new RuntimeException(
            "Cannot find the select field in the available fields: " + node.toString());
      }
      updateFunctionArgTypesAndParams(schemaField.get());
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
      Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, arrayBaseName);
      if (!schemaField.isPresent()) {
        throw new RuntimeException(
            "Cannot find the select field in the available fields: " + arrayBaseName);
      }
      updateFunctionArgTypesAndParams(schemaField.get());
      process(node.getIndex(), context);
      return null;
    }

    private void updateFunctionArgTypesAndParams(final Field schemaField) {
      final Schema schema = schemaField.schema();
      if (inFunctionCall()) {
        functionArgs.peek().add(schema.type());
      }
      parameterMap.put(
          schemaField.name().replace(".", "_"),
          new KsqlFunction(schema,
              Collections.emptyList(), schemaField.name(), SchemaUtil.getJavaType(schema))
      );
    }

    @Override
    protected Object visitQualifiedNameReference(QualifiedNameReference node, Object context) {
      Optional<Field> schemaField = SchemaUtil.getFieldByName(schema, node.getName().getSuffix());
      if (!schemaField.isPresent()) {
        throw new RuntimeException(
            "Cannot find the select field in the available fields: " + node.getName().getSuffix());
      }
      updateFunctionArgTypesAndParams(schemaField.get());
      return null;
    }

    private Object maybeUpdateFunctionArgs(final Schema.Type type) {
      if (inFunctionCall()) {
        functionArgs.peek().add(type);
      }
      return null;
    }

    @Override
    protected Object visitStringLiteral(final StringLiteral node, final Object context) {
      return maybeUpdateFunctionArgs(Schema.Type.STRING);
    }

    @Override
    protected Object visitDoubleLiteral(final DoubleLiteral node, final Object context) {
      return maybeUpdateFunctionArgs(Schema.Type.FLOAT64);
    }

    @Override
    protected Object visitLongLiteral(final LongLiteral node, final Object context) {
      return maybeUpdateFunctionArgs(Schema.Type.INT64);
    }

  }
}
