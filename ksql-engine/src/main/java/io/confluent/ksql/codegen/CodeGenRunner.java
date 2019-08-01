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

package io.confluent.ksql.codegen;

import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.parser.tree.ArithmeticBinaryExpression;
import io.confluent.ksql.parser.tree.ArithmeticUnaryExpression;
import io.confluent.ksql.parser.tree.BetweenPredicate;
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
import io.confluent.ksql.parser.tree.SearchedCaseExpression;
import io.confluent.ksql.parser.tree.SubscriptExpression;
import io.confluent.ksql.parser.tree.VisitParentExpressionVisitor;
import io.confluent.ksql.schema.ksql.Field;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToJavaTypeConverter;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.ExpressionTypeManager;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import io.confluent.ksql.util.SchemaUtil;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public class CodeGenRunner {

  private final LogicalSchema schema;
  private final FunctionRegistry functionRegistry;
  private final ExpressionTypeManager expressionTypeManager;
  private final KsqlConfig ksqlConfig;

  public static List<ExpressionMetadata> compileExpressions(
      final Stream<Expression> expressions,
      final String type,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(schema, ksqlConfig, functionRegistry);

    return expressions
        .map(exp -> codeGen.buildCodeGenFromParseTree(exp, type))
        .collect(Collectors.toList());
  }

  public CodeGenRunner(
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);
  }

  public Set<ParameterType> getParameterInfo(final Expression expression) {
    final Visitor visitor =
        new Visitor(schema, functionRegistry, expressionTypeManager, ksqlConfig);

    visitor.process(expression, null);
    return visitor.parameters;
  }

  public ExpressionMetadata buildCodeGenFromParseTree(
      final Expression expression,
      final String type
  ) {
    try {
      final Set<ParameterType> parameters = getParameterInfo(expression);

      final String[] parameterNames = new String[parameters.size()];
      final Class[] parameterTypes = new Class[parameters.size()];
      final List<Integer> columnIndexes = new ArrayList<>(parameters.size());
      final List<Kudf> kudfObjects = new ArrayList<>(parameters.size());

      int index = 0;
      for (final ParameterType param : parameters) {
        parameterNames[index] = param.paramName;
        parameterTypes[index] = param.type;
        columnIndexes.add(schema.valueFieldIndex(param.fieldName).orElse(-1));
        kudfObjects.add(param.getKudf());
        index++;
      }

      final String javaCode = new SqlToJavaVisitor(schema, functionRegistry).process(expression);

      final IExpressionEvaluator ee =
          CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
      ee.setDefaultImports(SqlToJavaVisitor.JAVA_IMPORTS.toArray(new String[0]));
      ee.setParameters(parameterNames, parameterTypes);

      final Schema expressionType = expressionTypeManager.getExpressionSchema(expression);

      ee.setExpressionType(SchemaUtil.getJavaType(expressionType));

      ee.cook(javaCode);

      return new ExpressionMetadata(
          ee,
          columnIndexes,
          kudfObjects,
          expressionType,
          new GenericRowValueTypeEnforcer(schema),
          expression);
    } catch (final KsqlException | CompileException e) {
      throw new KsqlException("Code generation failed for " + type
          + ": " + e.getMessage()
          + ". expression:" + expression + ", schema:" + schema, e);
    } catch (final Exception e) {
      throw new RuntimeException("Unexpected error generating code for " + type
          + ". expression:" + expression, e);
    }
  }

  private static final class Visitor extends VisitParentExpressionVisitor<Object, Object> {

    private static final SqlToJavaTypeConverter converter = SchemaConverters.sqlToJavaConverter();

    private final LogicalSchema schema;
    private final Set<ParameterType> parameters;
    private final FunctionRegistry functionRegistry;
    private final ExpressionTypeManager expressionTypeManager;
    private final KsqlConfig ksqlConfig;

    private int functionCounter = 0;

    private Visitor(
        final LogicalSchema schema,
        final FunctionRegistry functionRegistry,
        final ExpressionTypeManager expressionTypeManager,
        final KsqlConfig ksqlConfig
    ) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
      this.parameters = new HashSet<>();
      this.functionRegistry = functionRegistry;
      this.expressionTypeManager = expressionTypeManager;
    }

    private void addParameter(final Field schemaField) {
      parameters.add(new ParameterType(
          converter.toJavaType(schemaField.type().baseType()),
          schemaField.fullName(),
          schemaField.fullName().replace(".", "_"),
          ksqlConfig));
    }

    public Object visitLikePredicate(final LikePredicate node, final Object context) {
      process(node.getValue(), null);
      return null;
    }

    public Object visitFunctionCall(final FunctionCall node, final Object context) {
      final int functionNumber = functionCounter++;
      final List<Schema> argumentTypes = new ArrayList<>();
      final String functionName = node.getName().getSuffix();
      for (final Expression argExpr : node.getArguments()) {
        process(argExpr, null);
        argumentTypes.add(expressionTypeManager.getExpressionSchema(argExpr));
      }

      final UdfFactory holder = functionRegistry.getUdfFactory(functionName);
      final KsqlFunction function = holder.getFunction(argumentTypes);
      final String parameterName = node.getName().getSuffix() + "_" + functionNumber;
      parameters.add(new ParameterType(
          function,
          parameterName,
          parameterName,
          ksqlConfig));
      return null;
    }

    public Object visitArithmeticBinary(
        final ArithmeticBinaryExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    public Object visitArithmeticUnary(
        final ArithmeticUnaryExpression node,
        final Object context) {
      process(node.getValue(), null);
      return null;
    }

    public Object visitIsNotNullPredicate(final IsNotNullPredicate node, final Object context) {
      return process(node.getValue(), context);
    }

    public Object visitIsNullPredicate(final IsNullPredicate node, final Object context) {
      return process(node.getValue(), context);
    }

    public Object visitLogicalBinaryExpression(
        final LogicalBinaryExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    public Object visitComparisonExpression(
        final ComparisonExpression node,
        final Object context) {
      process(node.getLeft(), null);
      process(node.getRight(), null);
      return null;
    }

    @Override
    public Object visitBetweenPredicate(final BetweenPredicate node, final Object context) {
      process(node.getValue(), null);
      process(node.getMax(), null);
      process(node.getMin(), null);
      return null;
    }

    @Override
    public Object visitNotExpression(final NotExpression node, final Object context) {
      return process(node.getValue(), null);
    }

    @Override
    public Object visitDereferenceExpression(
        final DereferenceExpression node,
        final Object context
    ) {
      addParameter(getRequiredField(node.toString()));
      return null;
    }

    @Override
    public Object visitSearchedCaseExpression(
        final SearchedCaseExpression node,
        final Object context) {
      node.getWhenClauses().forEach(
          whenClause -> {
            process(whenClause.getOperand(), context);
            process(whenClause.getResult(), context);
          }
      );
      node.getDefaultValue().ifPresent(defaultVal -> process(defaultVal, context));
      return null;
    }

    @Override
    public Object visitCast(final Cast node, final Object context) {
      process(node.getExpression(), context);
      return null;
    }

    @Override
    public Object visitSubscriptExpression(
        final SubscriptExpression node,
        final Object context
    ) {
      if (node.getBase() instanceof DereferenceExpression
          || node.getBase() instanceof QualifiedNameReference) {
        final String arrayBaseName = node.getBase().toString();
        addParameter(getRequiredField(arrayBaseName));
      } else {
        process(node.getBase(), context);
      }
      process(node.getIndex(), context);
      return null;
    }

    @Override
    public Object visitQualifiedNameReference(
        final QualifiedNameReference node,
        final Object context
    ) {
      addParameter(getRequiredField(node.getName().getSuffix()));
      return null;
    }

    private Field getRequiredField(final String fieldName) {
      return schema.findValueField(fieldName)
          .orElseThrow(() -> new RuntimeException(
              "Cannot find the select field in the available fields."
                  + " field: " + fieldName
                  + ", schema: " + schema.valueFields()));
    }
  }

  public static final class ParameterType {

    private final Class type;
    private final Optional<KsqlFunction> function;
    private final String paramName;
    private final String fieldName;
    private final KsqlConfig ksqlConfig;

    private ParameterType(
        final Class type,
        final String fieldName,
        final String paramName,
        final KsqlConfig ksqlConfig
    ) {
      this(
          null,
          Objects.requireNonNull(type, "type"),
          fieldName,
          paramName,
          ksqlConfig);
    }

    private ParameterType(
        final KsqlFunction function,
        final String fieldName,
        final String paramName,
        final KsqlConfig ksqlConfig) {
      this(
          Objects.requireNonNull(function, "function"),
          function.getKudfClass(),
          fieldName,
          paramName,
          ksqlConfig);
    }

    private ParameterType(
        final KsqlFunction function,
        final Class type,
        final String fieldName,
        final String paramName,
        final KsqlConfig ksqlConfig
    ) {
      this.function = Optional.ofNullable(function);
      this.type = Objects.requireNonNull(type, "type");
      this.fieldName = Objects.requireNonNull(fieldName, "fieldName");
      this.paramName = Objects.requireNonNull(paramName, "paramName");
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    }

    public Class getType() {
      return type;
    }

    public String getParamName() {
      return paramName;
    }

    public String getFieldName() {
      return fieldName;
    }

    public Kudf getKudf() {
      return function.map(f -> f.newInstance(ksqlConfig)).orElse(null);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final ParameterType that = (ParameterType) o;
      return Objects.equals(type, that.type)
          && Objects.equals(function, that.function)
          && Objects.equals(paramName, that.paramName)
          && Objects.equals(fieldName, that.fieldName);
    }

    @Override
    public int hashCode() {
      return Objects.hash(type, function, paramName, fieldName);
    }
  }
}
