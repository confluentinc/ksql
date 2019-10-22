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
import io.confluent.ksql.execution.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToJavaTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.kafka.connect.data.Schema;
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

  public CodeGenSpec getCodeGenSpec(final Expression expression) {
    final Visitor visitor =
        new Visitor(schema, functionRegistry, expressionTypeManager, ksqlConfig);

    visitor.process(expression, null);
    return visitor.spec.build();
  }

  public ExpressionMetadata buildCodeGenFromParseTree(
      final Expression expression,
      final String type
  ) {
    try {
      final CodeGenSpec spec = getCodeGenSpec(expression);
      final String javaCode = SqlToJavaVisitor.of(
          schema,
          functionRegistry,
          spec
      ).process(expression);

      final IExpressionEvaluator ee =
          CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
      ee.setDefaultImports(SqlToJavaVisitor.JAVA_IMPORTS.toArray(new String[0]));
      ee.setParameters(spec.argumentNames(), spec.argumentTypes());

      final SqlType expressionType = expressionTypeManager
          .getExpressionSqlType(expression);

      ee.setExpressionType(SQL_TO_JAVA_TYPE_CONVERTER.toJavaType(expressionType));

      ee.cook(javaCode);

      return new ExpressionMetadata(
          ee,
          spec,
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

  private static final class Visitor extends TraversalExpressionVisitor<Void> {

    private final CodeGenSpec.Builder spec;
    private final LogicalSchema schema;
    private final FunctionRegistry functionRegistry;
    private final ExpressionTypeManager expressionTypeManager;
    private final KsqlConfig ksqlConfig;

    private Visitor(
        final LogicalSchema schema,
        final FunctionRegistry functionRegistry,
        final ExpressionTypeManager expressionTypeManager,
        final KsqlConfig ksqlConfig
    ) {
      this.schema = Objects.requireNonNull(schema, "schema");
      this.ksqlConfig = Objects.requireNonNull(ksqlConfig, "ksqlConfig");
      this.functionRegistry = functionRegistry;
      this.expressionTypeManager = expressionTypeManager;
      this.spec = new CodeGenSpec.Builder();
    }

    private void addParameter(final Column schemaColumn) {
      spec.addParameter(
          schemaColumn.ref(),
          SQL_TO_JAVA_TYPE_CONVERTER.toJavaType(schemaColumn.type()),
          schema.valueColumnIndex(schemaColumn.ref())
              .orElseThrow(() -> new KsqlException(
                  "Expected to find column in schema, but was missing: " + schemaColumn))
      );
    }

    public Void visitLikePredicate(final LikePredicate node, final Void context) {
      process(node.getValue(), null);
      return null;
    }

    @SuppressWarnings("deprecation") // Need to migrate away from Connect Schema use.
    public Void visitFunctionCall(final FunctionCall node, final Void context) {
      final List<Schema> argumentTypes = new ArrayList<>();
      final FunctionName functionName = node.getName();
      for (final Expression argExpr : node.getArguments()) {
        process(argExpr, null);
        argumentTypes.add(expressionTypeManager.getExpressionSchema(argExpr));
      }

      final UdfFactory holder = functionRegistry.getUdfFactory(functionName.name());
      final KsqlFunction function = holder.getFunction(argumentTypes);
      spec.addFunction(
          function.getFunctionName(),
          function.newInstance(ksqlConfig)
      );

      return null;
    }

    @Override
    public Void visitSubscriptExpression(
        final SubscriptExpression node,
        final Void context
    ) {
      if (node.getBase() instanceof ColumnReferenceExp) {
        final ColumnReferenceExp arrayBaseName = (ColumnReferenceExp) node.getBase();
        addParameter(getRequiredColumn(arrayBaseName.getReference()));
      } else {
        process(node.getBase(), context);
      }
      process(node.getIndex(), context);
      return null;
    }

    @Override
    public Void visitColumnReference(
        final ColumnReferenceExp node,
        final Void context
    ) {
      addParameter(getRequiredColumn(node.getReference()));
      return null;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Void visitDereferenceExpression(final DereferenceExpression node, final Void context) {
      process(node.getBase(), null);

      final List<Schema> argumentTypes = ImmutableList.of(
          expressionTypeManager.getExpressionSchema(node.getBase()),
          Schema.OPTIONAL_STRING_SCHEMA
      );

      final KsqlFunction function = functionRegistry
          .getUdfFactory(FetchFieldFromStruct.FUNCTION_NAME)
          .getFunction(argumentTypes);

      spec.addFunction(
          function.getFunctionName(),
          function.newInstance(ksqlConfig)
      );

      return null;
    }

    private Column getRequiredColumn(final ColumnRef target) {
      return schema.findValueColumn(target)
          .orElseThrow(() -> new RuntimeException(
              "Cannot find the select field in the available fields."
                  + " field: " + target
                  + ", schema: " + schema.value()));
    }
  }
}
