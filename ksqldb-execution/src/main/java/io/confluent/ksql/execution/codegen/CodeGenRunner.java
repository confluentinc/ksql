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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.execution.expression.tree.CreateArrayExpression;
import io.confluent.ksql.execution.expression.tree.CreateMapExpression;
import io.confluent.ksql.execution.expression.tree.CreateStructExpression;
import io.confluent.ksql.execution.expression.tree.DereferenceExpression;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.expression.tree.FunctionCall;
import io.confluent.ksql.execution.expression.tree.LambdaFunctionCall;
import io.confluent.ksql.execution.expression.tree.LikePredicate;
import io.confluent.ksql.execution.expression.tree.SubscriptExpression;
import io.confluent.ksql.execution.expression.tree.TraversalExpressionVisitor;
import io.confluent.ksql.execution.expression.tree.UnqualifiedColumnReferenceExp;
import io.confluent.ksql.execution.util.ExpressionTypeManager;
import io.confluent.ksql.execution.util.FunctionArgumentsUtil;
import io.confluent.ksql.execution.util.FunctionArgumentsUtil.ArgumentInfo;
import io.confluent.ksql.execution.util.FunctionArgumentsUtil.FunctionTypeInfo;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.KsqlScalarFunction;
import io.confluent.ksql.function.UdfFactory;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.SchemaConverters;
import io.confluent.ksql.schema.ksql.SchemaConverters.SqlToJavaTypeConverter;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
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

  public static List<CompiledExpression> compileExpressions(
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

  public static CompiledExpression compileExpression(
      final Expression expression,
      final String type,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    final CodeGenRunner codeGen = new CodeGenRunner(schema, ksqlConfig, functionRegistry);
    return compileExpression(expression, type, codeGen);
  }

  private static CompiledExpression compileExpression(
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
    final TypeContext context = new TypeContext();
    visitor.process(expression, context);
    return visitor.spec.build();
  }

  public CompiledExpression buildCodeGenFromParseTree(
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

      final SqlType returnType = expressionTypeManager.getExpressionSqlType(expression);
      if (returnType == null) {
        // expressionType can be null if expression is NULL.
        throw new KsqlException("NULL expression not supported");
      }

      final Class<?> expressionType = SQL_TO_JAVA_TYPE_CONVERTER.toJavaType(returnType);

      final IExpressionEvaluator ee =
          cook(javaCode, expressionType, spec.argumentNames(), spec.argumentTypes());

      return new CompiledExpression(ee, spec, returnType, expression);
    } catch (KsqlException | CompileException e) {
      throw new KsqlException("Invalid " + type + ": " + e.getMessage()
          + ". expression:" + expression + ", schema:" + schema, e);
    } catch (final Exception e) {
      throw new RuntimeException("Unexpected error generating code for " + type
          + ". expression:" + expression, e);
    }
  }

  @VisibleForTesting
  public static IExpressionEvaluator cook(
      final String javaCode,
      final Class<?> expressionType,
      final String[] argNames,
      final Class<?>[] argTypes
  ) throws Exception {
    final IExpressionEvaluator ee = CompilerFactoryFactory.getDefaultCompilerFactory()
        .newExpressionEvaluator();

    ee.setDefaultImports(SqlToJavaVisitor.JAVA_IMPORTS.toArray(new String[0]));
    ee.setParameters(argNames, argTypes);
    ee.setExpressionType(expressionType);
    ee.cook(javaCode);
    return ee;
  }

  private final class Visitor extends TraversalExpressionVisitor<TypeContext> {

    private final CodeGenSpec.Builder spec;

    private Visitor() {
      this.spec = new CodeGenSpec.Builder();
    }

    @Override
    public Void visitLikePredicate(final LikePredicate node, final TypeContext context) {
      process(node.getValue(), context);
      process(node.getPattern(), context);
      return null;
    }

    @Override
    public Void visitFunctionCall(final FunctionCall node, final TypeContext context) {
      final UdfFactory udfFactory = functionRegistry.getUdfFactory(node.getName());
      final FunctionTypeInfo argumentsAndContext = FunctionArgumentsUtil
          .getFunctionTypeInfo(
              expressionTypeManager,
              node,
              udfFactory,
              context);

      final List<Expression> arguments = node.getArguments();
      final List<TypeContext> typeContextsForChildren =
          argumentsAndContext.getArgumentInfos().stream()
              .map(ArgumentInfo::getContext)
              .collect(Collectors.toList());
      final KsqlScalarFunction function = argumentsAndContext.getFunction();

      spec.addFunction(
          function.name(),
          function.newInstance(ksqlConfig)
      );
      for (int i = 0; i < arguments.size(); i++) {
        process(arguments.get(i), typeContextsForChildren.get(i));
      }
      return null;
    }

    @Override
    public Void visitSubscriptExpression(
        final SubscriptExpression node, final TypeContext context
    ) {
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
    public Void visitCreateArrayExpression(
        final CreateArrayExpression exp, final TypeContext context
    ) {
      exp.getValues().forEach(val -> process(val, context));
      return null;
    }

    @Override
    public Void visitCreateMapExpression(final CreateMapExpression exp, final TypeContext context) {
      for (Entry<Expression, Expression> entry : exp.getMap().entrySet()) {
        process(entry.getKey(), context);
        process(entry.getValue(), context);
      }
      return null;
    }

    @Override
    public Void visitStructExpression(
        final CreateStructExpression exp,
        final TypeContext context
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
        final TypeContext context
    ) {
      addRequiredColumn(node.getColumnName());
      return null;
    }

    @Override
    public Void visitDereferenceExpression(
        final DereferenceExpression node, final TypeContext context
    ) {
      process(node.getBase(), context);
      return null;
    }

    @Override
    public Void visitLambdaExpression(final LambdaFunctionCall node, final TypeContext context) {
      process(node.getBody(), context);
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
