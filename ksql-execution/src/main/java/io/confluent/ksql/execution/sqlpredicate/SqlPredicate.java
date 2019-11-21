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

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.CodeGenSpec;
import io.confluent.ksql.execution.codegen.CodeGenSpec.ArgumentSpec;
import io.confluent.ksql.execution.codegen.SqlToJavaVisitor;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import org.apache.kafka.streams.kstream.Predicate;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IExpressionEvaluator;

public final class SqlPredicate {

  private final Expression filterExpression;
  private final IExpressionEvaluator ee;
  private final CodeGenSpec spec;

  public SqlPredicate(
      Expression filterExpression,
      LogicalSchema schema,
      KsqlConfig ksqlConfig,
      FunctionRegistry functionRegistry
  ) {
    this.filterExpression = requireNonNull(filterExpression, "filterExpression");

    CodeGenRunner codeGenRunner = new CodeGenRunner(schema, ksqlConfig, functionRegistry);
    spec = codeGenRunner.getCodeGenSpec(this.filterExpression);

    try {
      ee = CompilerFactoryFactory.getDefaultCompilerFactory().newExpressionEvaluator();
      ee.setDefaultImports(SqlToJavaVisitor.JAVA_IMPORTS.toArray(new String[0]));
      ee.setParameters(spec.argumentNames(), spec.argumentTypes());

      ee.setExpressionType(boolean.class);

      String expressionStr = SqlToJavaVisitor.of(
          schema,
          functionRegistry,
          spec
      ).process(this.filterExpression);

      ee.cook(expressionStr);
    } catch (Exception e) {
      throw new KsqlException(
          "Failed to generate code for SqlPredicate."
              + " filterExpression: " + filterExpression
              + ", schema:" + schema,
          e
      );
    }
  }

  public <K> Predicate<K, GenericRow> getPredicate(final ProcessingLogger processingLogger) {
    Objects.requireNonNull(processingLogger, "processingLogger");

    return (key, row) -> {
      if (row == null) {
        return false;
      }

      try {
        Object[] values = new Object[spec.arguments().size()];
        spec.resolve(row, values);
        return (Boolean) ee.evaluate(values);
      } catch (Exception e) {
        logProcessingError(processingLogger, e, row);
      }
      return false;
    };
  }

  Expression getFilterExpression() {
    return filterExpression;
  }

  @VisibleForTesting
  int[] getColumnIndexes() {
    // As this is only used for testing it is ok to do the array copy.
    // We need to revisit the tests for this class and remove this.
    return spec.arguments()
        .stream()
        .map(ArgumentSpec::colIndex)
        .mapToInt(idx -> idx.orElse(-1))
        .toArray();
  }

  private void logProcessingError(
      final ProcessingLogger processingLogger,
      final Exception e,
      final GenericRow row
  ) {
    final String msg = String.format(
        "Error evaluating predicate %s: %s",
        filterExpression,
        e.getMessage()
    );

    processingLogger.error(EngineProcessingLogMessageFactory.recordProcessingError(msg, e, row));
  }
}
