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

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.FunctionName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToJavaTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
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
        .map(exp -> compileExpression(exp, type, codeGen))
        .collect(Collectors.toList());
  }

  public static ExpressionMetadata compileExpression(
      final Expression expression,
      final String type,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(schema, ksqlConfig, functionRegistry);
    return compileExpression(expression, type, codeGen);
  }

  private static ExpressionMetadata compileExpression(
      final Expression expression,
      final String type,
      final CodeGenRunner codeGen
  ) {
    return codeGen.buildCodeGenFromParseTree(expression, type);
  }

  public CodeGenRunner(
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.schema = requireNonNull(schema, "schema");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.expressionTypeManager = new ExpressionTypeManager(schema, functionRegistry);
  }

  public CodeGenSpec getCodeGenSpec(final Expression expression) {
    final Visitor visitor = new Visitor();

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
          spec,
          ksqlConfig
      ).process(expression);

      final IExpressionEvaluator ee =
          CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
      ee.setDefaultImports(SqlToJavaVisitor.JAVA_IMPORTS.toArray(new String[0]));
      ee.setParameters(spec.argumentNames(), spec.argumentTypes());

      final SqlType expressionType = expressionTypeManager
          .getExpressionSqlType(expression);

      if (expressionType == null) {
        // expressionType can be null if expression is NULL.
        throw new KsqlException("NULL expression not supported");
      }

      ee.setExpressionType(SQL_TO_JAVA_TYPE_CONVERTER.toJavaType(expressionType));

      ee.cook(javaCode);

      return new ExpressionMetadata(
          ee,
          spec,
          expressionType,
          expression
      );
    } catch (KsqlException | CompileException e) {
      throw new KsqlException("Invalid " + type + ": " + e.getMessage()
          + ". expression:" + expression + ", schema:" + schema, e);
    } catch (final Exception e) {
      throw new RuntimeException("Unexpected error generating code for " + type
          + ". expression:" + expression, e);
    }
  }

  private final class Visitor extends TraversalExpressionVisitor<Void> {

    private final CodeGenSpec.Builder spec;

    private Visitor() {
      this.spec = new CodeGenSpec.Builder();
    }

    @Override
    public Void visitLikePredicate(final LikePredicate node, final Void context) {
      process(node.getValue(), null);
      process(node.getPattern(), null);
      return null;
    }

    @Override
    public Void visitFunctionCall(final FunctionCall node, final Void context) {
      final List<SqlType> argumentTypes = new ArrayList<>();
      final FunctionName functionName = node.getName();
      for (final Expression argExpr : node.getArguments()) {
        process(argExpr, null);
        argumentTypes.add(expressionTypeManager.getExpressionSqlType(argExpr));
      }

      final UdfFactory holder = functionRegistry.getUdfFactory(functionName);
      final KsqlScalarFunction function = holder.getFunction(argumentTypes);
      spec.addFunction(
          function.name(),
          function.newInstance(ksqlConfig)
      );

      return null;
    }

    @Override
    public Void visitSubscriptExpression(final SubscriptExpression node, final Void context) {
      if (node.getBase() instanceof UnqualifiedColumnReferenceExp) {
        final UnqualifiedColumnReferenceExp arrayBaseName
            = (UnqualifiedColumnReferenceExp) node.getBase();
        addRequiredColumn(arrayBaseName.getColumnName());
      } else {
        process(node.getBase(), context);
      }
      process(node.getIndex(), context);
      return null;
    }

    @Override
    public Void visitCreateArrayExpression(final CreateArrayExpression exp, final Void context) {
      exp.getValues().forEach(val -> process(val, context));
      return null;
    }

    @Override
    public Void visitCreateMapExpression(final CreateMapExpression exp, final Void context) {
      for (Entry<Expression, Expression> entry : exp.getMap().entrySet()) {
        process(entry.getKey(), context);
        process(entry.getValue(), context);
      }
      return null;
    }

    @Override
    public Void visitStructExpression(
        final CreateStructExpression exp,
        final Void context
    ) {
      exp.getFields().forEach(val -> process(val.getValue(), context));
      final Schema schema = SchemaConverters
          .sqlToConnectConverter()
          .toConnectSchema(expressionTypeManager.getExpressionSqlType(exp));

      spec.addStructSchema(exp, schema);
      return null;
    }

    @Override
    public Void visitUnqualifiedColumnReference(
        final UnqualifiedColumnReferenceExp node,
        final Void context
    ) {
      addRequiredColumn(node.getColumnName());
      return null;
    }

    @Override
    public Void visitDereferenceExpression(final DereferenceExpression node, final Void context) {
      process(node.getBase(), null);
      return null;
    }

    private void addRequiredColumn(final ColumnName columnName) {
      final Column column = schema.findValueColumn(columnName)
          .orElseThrow(() -> new KsqlException(
              "Cannot find the select field in the available fields."
                  + " field: " + columnName
                  + ", schema: " + schema.value()));

      spec.addParameter(
          column.name(),
          SQL_TO_JAVA_TYPE_CONVERTER.toJavaType(column.type()),
          column.index()
      );
    }
  }
}
