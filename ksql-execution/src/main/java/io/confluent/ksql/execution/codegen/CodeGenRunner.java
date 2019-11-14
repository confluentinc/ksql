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

package io.confluent.ksql.execution.codegen;

import com.google.common.collect.ImmutableList;
import io.confluent.ksql.execution.expression.tree.ColumnReferenceExp;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.function.udf.structfieldextractor.FetchFieldFromStruct;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToJavaTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public class CodeGenRunner {

  private static final SqlToJavaTypeConverter SQL_TO_JAVA_TYPE_CONVERTER =
      SchemaConverters.sqlToJavaConverter();

  private final LogicalSchema schema;
  private final FunctionRegistry functionRegistry;
  private final ExpressionTypeManager expressionTypeManager;
  private final KsqlConfig ksqlConfig;

  public static List<ExpressionMetadata> compileExpressions(
      Stream<Expression> expressions, String type, LogicalSchema schema, KsqlConfig ksqlConfig,
      FunctionRegistry functionRegistry
  ) {
    CodeGenRunner codeGen = new CodeGenRunner(schema, ksqlConfig, functionRegistry);

    return expressions
        .map(exp -> codeGen.buildCodeGenFromParseTree(exp, type))
        .collect(Collectors.toList());
  }

  public CodeGenRunner(
      LogicalSchema schema, KsqlConfig ksqlConfig, FunctionRegistry functionRegistry
  ) {
    this.functionRegistry = Objects.requireNonNull(functionRegistry, "functionRegistry");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
    this.expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);
  }

  public CodeGenSpec getCodeGenSpec(Expression expression) {
    Visitor visitor =
        new Visitor(schema, functionRegistry, expressionTypeManager, ksqlConfig);

    visitor.process(expression, null);
    return visitor.spec.build();
  }

  public ExpressionMetadata buildCodeGenFromParseTree(Expression expression, String type) {
    try {
      CodeGenSpec spec = getCodeGenSpec(expression);
      String javaCode = SqlToJavaVisitor.of(
          schema,
          functionRegistry,
          spec
      ).process(expression);

      IExpressionEvaluator ee =
          CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
      ee.setDefaultImports(SqlToJavaVisitor.JAVA_IMPORTS.toArray(new String[0]));
      ee.setParameters(spec.argumentNames(), spec.argumentTypes());

      SqlType expressionType = expressionTypeManager
          .getExpressionSqlType(expression);

      ee.setExpressionType(SQL_TO_JAVA_TYPE_CONVERTER.toJavaType(expressionType));

      ee.cook(javaCode);

      return new ExpressionMetadata(
          ee,
          spec,
          expressionType,
          expression
      );
    } catch (KsqlException | CompileException e) {
      throw new KsqlException("Code generation failed for " + type
          + ": " + e.getMessage()
          + ". expression:" + expression + ", schema:" + schema, e);
    } catch (Exception e) {
      throw new RuntimeException("Unexpected error generating code for " + type
          + ". expression:" + expression, e);
    }
  }

  private static final class Visitor extends TraversalExpressionVisitor<Void> {

    private final CodeGenSpec.Builder spec;
    private final LogicalSchema schema;
    private final FunctionRegistry functionRegistry;
    private final ExpressionTypeManager expressionTypeManager;
    private final KsqlConfig ksqlConfig;

    private Visitor(
        LogicalSchema schema, FunctionRegistry functionRegistry,
        ExpressionTypeManager expressionTypeManager, KsqlConfig ksqlConfig
    ) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
      this.functionRegistry = functionRegistry;
      this.expressionTypeManager = expressionTypeManager;
      this.spec = new CodeGenSpec.Builder();
    }

    private void addParameter(Column schemaColumn) {
      spec.addParameter(
          schemaColumn.ref(),
          SQL_TO_JAVA_TYPE_CONVERTER.toJavaType(schemaColumn.type()),
          schema.valueColumnIndex(schemaColumn.ref())
              .orElseThrow(() -> new KsqlException(
                  "Expected to find column in schema, but was missing: " + schemaColumn))
      );
    }

    public Void visitLikePredicate(LikePredicate node, Void context) {
      process(node.getValue(), null);
      return null;
    }

    @SuppressWarnings("deprecation") // Need to migrate away from Connect Schema use.
    public Void visitFunctionCall(FunctionCall node, Void context) {
      List<SqlType> argumentTypes = new ArrayList<>();
      FunctionName functionName = node.getName();
      for (Expression argExpr : node.getArguments()) {
        process(argExpr, null);
        argumentTypes.add(expressionTypeManager.getExpressionSqlType(argExpr));
      }

      UdfFactory holder = functionRegistry.getUdfFactory(functionName.name());
      KsqlScalarFunction function = holder.getFunction(argumentTypes);
      spec.addFunction(
          function.name(),
          function.newInstance(ksqlConfig)
      );

      return null;
    }

    @Override
    public Void visitSubscriptExpression(SubscriptExpression node, Void context) {
      if (node.getBase() instanceof ColumnReferenceExp) {
        ColumnReferenceExp arrayBaseName = (ColumnReferenceExp) node.getBase();
        addParameter(getRequiredColumn(arrayBaseName.getReference()));
      } else {
        process(node.getBase(), context);
      }
      process(node.getIndex(), context);
      return null;
    }

    @Override
    public Void visitColumnReference(ColumnReferenceExp node, Void context) {
      addParameter(getRequiredColumn(node.getReference()));
      return null;
    }

    @Override
    public Void visitDereferenceExpression(DereferenceExpression node, Void context) {
      process(node.getBase(), null);

      ImmutableList<SqlType> argumentTypes = ImmutableList.of(
          expressionTypeManager.getExpressionSqlType(node.getBase()),
          SqlTypes.STRING
      );

      KsqlScalarFunction function = functionRegistry
          .getUdfFactory(FetchFieldFromStruct.FUNCTION_NAME.name())
          .getFunction(argumentTypes);

      spec.addFunction(
          function.name(),
          function.newInstance(ksqlConfig)
      );

      return null;
    }

    private Column getRequiredColumn(ColumnRef target) {
      return schema.findValueColumn(target)
          .orElseThrow(() -> new RuntimeException(
              "Cannot find the select field in the available fields."
                  + " field: " + target
                  + ", schema: " + schema.value()));
    }
  }
}
