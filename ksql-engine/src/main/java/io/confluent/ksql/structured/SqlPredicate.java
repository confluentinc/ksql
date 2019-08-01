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

package io.confluent.ksql.structured;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.codegen.CodeGenRunner;
import io.confluent.ksql.codegen.SqlToJavaVisitor;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.parser.rewrite.StatementRewriteForRowtime;
import io.confluent.ksql.parser.tree.Expression;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.util.ExpressionMetadata;
import io.confluent.ksql.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Set;
import org.apache.kafka.streams.kstream.Predicate;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

class SqlPredicate {

  private final Expression filterExpression;
  private final LogicalSchema schema;
  private final IExpressionEvaluator ee;
  private final int[] columnIndexes;
  private final KsqlConfig ksqlConfig;
  private final FunctionRegistry functionRegistry;
  private final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;
  private final ProcessingLogger processingLogger;

  SqlPredicate(
      final Expression filterExpression,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogger processingLogger
  ) {
    this.filterExpression = rewriteFilter(requireNonNull(filterExpression, "filterExpression"));
    this.schema = requireNonNull(schema, "schema");
    this.genericRowValueTypeEnforcer = new GenericRowValueTypeEnforcer(schema);
    this.functionRegistry = requireNonNull(functionRegistry, "functionRegistry");
    this.ksqlConfig = requireNonNull(ksqlConfig, "ksqlConfig");
    this.processingLogger = requireNonNull(processingLogger);

    final CodeGenRunner codeGenRunner = new CodeGenRunner(schema, ksqlConfig, functionRegistry);
    final Set<CodeGenRunner.ParameterType> parameters
        = codeGenRunner.getParameterInfo(this.filterExpression);

    final String[] parameterNames = new String[parameters.size()];
    final Class[] parameterTypes = new Class[parameters.size()];
    columnIndexes = new int[parameters.size()];
    int index = 0;
    for (final CodeGenRunner.ParameterType param : parameters) {
      parameterNames[index] = param.getParamName();
      parameterTypes[index] = param.getType();
      columnIndexes[index] = schema.valueFieldIndex(param.getFieldName()).orElse(-1);
      index++;
    }

    try {
      ee = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
      ee.setDefaultImports(SqlToJavaVisitor.JAVA_IMPORTS.toArray(new String[0]));
      ee.setParameters(parameterNames, parameterTypes);

      ee.setExpressionType(boolean.class);

      final String expressionStr = new SqlToJavaVisitor(
          schema,
          functionRegistry
      ).process(this.filterExpression);

      ee.cook(expressionStr);
    } catch (final Exception e) {
      throw new KsqlException(
          "Failed to generate code for SqlPredicate."
          + " filterExpression: " + filterExpression
          + ", schema:" + schema,
          e
      );
    }
  }

  private Expression rewriteFilter(final Expression expression) {
    if (StatementRewriteForRowtime.requiresRewrite(expression)) {
      return new StatementRewriteForRowtime(expression).rewriteForRowtime();
    }
    return expression;
  }


  <K> Predicate<K, GenericRow> getPredicate() {
    final ExpressionMetadata expressionEvaluator = createExpressionMetadata();

    return (key, row) -> {
      if (row == null) {
        return false;
      }
      try {
        final List<Kudf> kudfs = expressionEvaluator.getUdfs();
        final Object[] values = new Object[columnIndexes.length];
        for (int i = 0; i < values.length; i++) {
          if (columnIndexes[i] < 0) {
            values[i] = kudfs.get(i);
          } else {
            values[i] = genericRowValueTypeEnforcer.enforceFieldType(
                columnIndexes[i],
                row.getColumns().get(columnIndexes[i])
            );
          }
        }
        return (Boolean) ee.evaluate(values);
      } catch (final Exception e) {
        logProcessingError(e, row);
      }
      return false;
    };
  }

  private ExpressionMetadata createExpressionMetadata() {
    final CodeGenRunner codeGenRunner = new CodeGenRunner(schema, ksqlConfig, functionRegistry);
    return codeGenRunner.buildCodeGenFromParseTree(filterExpression, "filter");
  }

  private void logProcessingError(final Exception e, final GenericRow row) {
    processingLogger.error(
        EngineProcessingLogMessageFactory.recordProcessingError(
            String.format(
                "Error evaluating predicate %s: %s",
                filterExpression,
                e.getMessage()
            ),
            e,
            row
        )
    );
  }

  Expression getFilterExpression() {
    return filterExpression;
  }

  // visible for testing
  int[] getColumnIndexes() {
    // As this is only used for testing it is ok to do the array copy.
    // We need to revisit the tests for this class and remove this.
    final int[] result = new int[columnIndexes.length];
    System.arraycopy(columnIndexes, 0, result, 0, columnIndexes.length);
    return result;
  }

}
