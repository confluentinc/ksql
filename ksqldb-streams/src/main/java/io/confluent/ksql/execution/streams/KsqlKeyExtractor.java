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

import static com.google.common.base.Preconditions.checkArgument;

import io.confluent.ksql.GenericKey;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.transform.ExpressionEvaluator;
import io.confluent.ksql.logging.processing.ProcessingLogger;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public final class KsqlKeyExtractor<KRightT> implements Function<GenericRow, KRightT> {

  private final int leftJoinColumnIndex;

  private final Optional<ExpressionEvaluator> expressionEvaluator;
  private final Optional<ProcessingLogger> processingLogger;
  private final Optional<Supplier<String>> errorMessage;

  KsqlKeyExtractor(final int leftJoinColumnIndex) {
    checkArgument(
        leftJoinColumnIndex >= 0,
        "leftJoinColumnIndex negative: " + leftJoinColumnIndex
    );

    this.leftJoinColumnIndex = leftJoinColumnIndex;
    expressionEvaluator = Optional.empty();
    processingLogger = Optional.empty();
    errorMessage = Optional.empty();
  }

  KsqlKeyExtractor(final ExpressionEvaluator expressionEvaluator,
                   final ProcessingLogger processingLogger) {
    leftJoinColumnIndex = -1;

    this.expressionEvaluator = Optional.of(expressionEvaluator);
    this.processingLogger = Optional.of(processingLogger);
    errorMessage = Optional.of(() -> String.format(
        "Error calculating left join expression: `%s`.",
        expressionEvaluator.getExpression()
    ));
  }

  @SuppressWarnings("unchecked")
  @Override
  public KRightT apply(final GenericRow left) {
    return expressionEvaluator
        .map(
            evaluator -> (KRightT) GenericKey.genericKey(
                evaluator.evaluate(
                    left,
                    null,
                    processingLogger.get(),
                    errorMessage.get()
                ))
        ).orElseGet(
            () -> (KRightT) GenericKey.genericKey(left.get(leftJoinColumnIndex))
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
    return expressionEvaluator == that.expressionEvaluator;
  }

  @Override
  public int hashCode() {
    return Objects.hash(expressionEvaluator);
  }

  @Override
  public String toString() {
    return "KsqlKeyExtractor{"
        + "expressionEvaluator=" + expressionEvaluator
        + '}';
  }
}
