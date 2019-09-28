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

package io.confluent.ksql.execution.sqlpredicate;

import static java.util.Objects.requireNonNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.CodeGenUtil;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.codegen.SqlToJavaVisitor;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.execution.util.GenericRowValueTypeEnforcer;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.function.udf.Kudf;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Set;
import org.apache.kafka.streams.kstream.Predicate;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public final class SqlPredicate {
  private final Expression filterExpression;
  private final LogicalSchema schema;
  private final IExpressionEvaluator ee;
  private final int[] columnIndexes;
  private final KsqlConfig ksqlConfig;
  private final FunctionRegistry functionRegistry;
  private final GenericRowValueTypeEnforcer genericRowValueTypeEnforcer;
  private final ProcessingLogger processingLogger;

  public SqlPredicate(
      final Expression filterExpression,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry,
      final ProcessingLogger processingLogger
  ) {
    this.filterExpression = requireNonNull(filterExpression, "filterExpression");
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

    final Builder<String, String> fieldToParamName = ImmutableMap.<String, String>builder();
    for (final CodeGenRunner.ParameterType param : parameters) {
      final String paramName = CodeGenUtil.paramName(index);
      fieldToParamName.put(param.getFieldName(), paramName);
      parameterNames[index] = paramName;
      parameterTypes[index] = param.getType();
      columnIndexes[index] = schema.valueColumnIndex(param.getFieldName()).orElse(-1);
      index++;
    }

    try {
      ee = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
      ee.setDefaultImports(SqlToJavaVisitor.JAVA_IMPORTS.toArray(new String[0]));
      ee.setParameters(parameterNames, parameterTypes);

      ee.setExpressionType(boolean.class);

      final String expressionStr = new SqlToJavaVisitor(
          schema,
          functionRegistry,
          fieldToParamName.build()::get
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

  public <K> Predicate<K, GenericRow> getPredicate() {
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
            values[i] = genericRowValueTypeEnforcer.enforceColumnType(
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
