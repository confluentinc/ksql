/*
 * Copyright 2021 Confluent Inc.
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

package io.confluent.ksql.execution.streams;

import static java.util.Objects.requireNonNull;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public final class KsqlKeyExtractor<KRightT> implements Function<GenericRow, KRightT> {

  private final ExpressionEvaluator expressionEvaluator;
  private final ProcessingLogger  processingLogger;
  private final Supplier<String> errorMessage;

  KsqlKeyExtractor(final ExpressionEvaluator expressionEvaluator,
                   final ProcessingLogger processingLogger) {
    this.expressionEvaluator = requireNonNull(expressionEvaluator);
    this.processingLogger = processingLogger;
    errorMessage = () -> String.format(
        "Error calculating left join expression: `%s`.",
        expressionEvaluator.getExpression()
    );
  }

  @SuppressWarnings("unchecked")
  @Override
  public KRightT apply(final GenericRow left) {
    return (KRightT) GenericKey.genericKey(
        expressionEvaluator.evaluate(
            left,
            null,
            processingLogger,
            errorMessage
        )
    );
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlKeyExtractor that = (KsqlKeyExtractor) o;
    return Objects.equals(expressionEvaluator, that.expressionEvaluator)
           && Objects.equals(processingLogger, that.processingLogger)
           && Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(expressionEvaluator, processingLogger, errorMessage);
  }

  @Override
  public String toString() {
    return "KsqlKeyExtractor{"
        + "expressionEvaluator=" + expressionEvaluator
        + "processingLogger=" + processingLogger
        + "errorMessage=" + errorMessage
        + '}';
  }
}
