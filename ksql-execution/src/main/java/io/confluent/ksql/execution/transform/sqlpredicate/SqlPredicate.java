/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.transform.sqlpredicate;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.CodeGenSpec;
import io.confluent.ksql.execution.codegen.SqlToJavaVisitor;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.KsqlConfig;
import io.confluent.ksql.util.KsqlException;
import java.util.Optional;
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

  public <K> KsqlTransformer<K, Optional<GenericRow>> getTransformer(
      final ProcessingLogger processingLogger
  ) {
    return new Transformer<>(processingLogger);
  }

  private final class Transformer<K> implements KsqlTransformer<K, Optional<GenericRow>> {

    private final ProcessingLogger processingLogger;
    private final String errorMsg;

    Transformer(final ProcessingLogger processingLogger) {
      this.processingLogger = requireNonNull(processingLogger, "processingLogger");
      this.errorMsg = "Error evaluating predicate "
          + SqlPredicate.this.filterExpression.toString()
          + ": ";
    }

    @Override
    public Optional<GenericRow> transform(
        final K readOnlyKey,
        final GenericRow value,
        final KsqlProcessingContext ctx
    ) {
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
      processingLogger.error(
          EngineProcessingLogMessageFactory
              .recordProcessingError(errorMsg + e.getMessage(), e, row)
      );
    }
  }
}
