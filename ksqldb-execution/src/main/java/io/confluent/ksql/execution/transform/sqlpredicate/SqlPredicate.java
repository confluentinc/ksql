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

import com.google.common.collect.Iterables;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.codegen.CodeGenRunner;
import io.confluent.ksql.execution.codegen.ExpressionMetadata;
import io.confluent.ksql.execution.expression.tree.Expression;
import io.confluent.ksql.execution.transform.KsqlProcessingContext;
import io.confluent.ksql.execution.transform.KsqlTransformer;
import io.confluent.ksql.execution.util.EngineProcessingLogMessageFactory;
import io.confluent.ksql.function.FunctionRegistry;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlTypes;
import io.confluent.ksql.util.KsqlConfig;
import java.util.Optional;
import java.util.stream.Stream;

public final class SqlPredicate {

  private final Expression filterExpression;
  private final ExpressionMetadata evaluator;

  public SqlPredicate(
      final Expression filterExpression,
      final LogicalSchema schema,
      final KsqlConfig ksqlConfig,
      final FunctionRegistry functionRegistry
  ) {
    this.filterExpression = requireNonNull(filterExpression, "filterExpression");

    this.evaluator = Iterables.getOnlyElement(CodeGenRunner.compileExpressions(
        Stream.of(filterExpression),
        "Predicate",
        schema,
        ksqlConfig,
        functionRegistry
    ));

    if (!this.evaluator.getExpressionType().equals(SqlTypes.BOOLEAN)) {
      throw new IllegalArgumentException(
          "Filter expression must resolve to boolean: " + filterExpression);
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
        final boolean result = (Boolean) evaluator.evaluate(value);
        return result
            ? Optional.of(value)
            : Optional.empty();

      } catch (final Exception e) {
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
