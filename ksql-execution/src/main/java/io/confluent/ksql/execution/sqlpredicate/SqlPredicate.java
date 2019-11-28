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

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.CodeGenSpec;
import io.confluent.ksql.execution.codegen.SqlToJavaVisitor;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.transform.KsqlValueTransformerWithKey;
import io.confluent.ksql.execution.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
import org.apache.kafka.streams.kstream.ValueTransformerWithKey;
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

  public <K> ValueTransformerWithKey<K, GenericRow, Optional<GenericRow>> getTransformer(
      final ProcessingLogger processingLogger
  ) {
    return new Transformer<>(processingLogger);
  }

  private final class Transformer<K> extends KsqlValueTransformerWithKey<K, Optional<GenericRow>> {

    private final ProcessingLogger processingLogger;

    Transformer(final ProcessingLogger processingLogger) {
      this.processingLogger = requireNonNull(processingLogger, "processingLogger");
    }

    @Override
    protected Optional<GenericRow> transform(final GenericRow value) {
      if (value == null) {
        return Optional.empty();
      }

      try {
        Object[] values = new Object[spec.arguments().size()];
        spec.resolve(value, values);
        final boolean result = (Boolean) ee.evaluate(values);
        return result
            ? Optional.of(value)
            : Optional.empty();

      } catch (Exception e) {
        logProcessingError(processingLogger, e, value);
        return Optional.empty();
      }
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
}
