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

package io.confluent.ksql.schema.ksql;

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

/**
 * Coerces values to {@link SqlBaseType SQL types}.
 */
public interface SqlValueCoercer {

  /**
   * Coerce the supplied {@code value} to the supplied {@code sqlType}.
   *
   * @param value the value to try to coerce.
   * @param targetSchema the target SQL type.
   * @return the Result of the coercion.
   */
  Result coerce(Object value, SqlType targetSchema);

  class Result {

    private final Optional<Optional<?>> result;

    public static Result failure() {
      return new Result(Optional.empty());
    }

    public static Result nullResult() {
      return new Result(Optional.of(Optional.empty()));
    }

    public static Result of(final Object result) {
      return new Result(Optional.of(Optional.of(result)));
    }

    private Result(final Optional<Optional<?>> result) {
      this.result = result;
    }

    public boolean failed() {
      return !result.isPresent();
    }

    public Optional<?> value() {
      return result.orElseThrow(IllegalStateException::new);
    }

    public <X extends Throwable> Optional<?> orElseThrow(
        final Supplier<? extends X> exceptionSupplier
    ) throws X {
      return result.orElseThrow(exceptionSupplier);
    }

    @Override
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      final Result result1 = (Result) o;
      return Objects.equals(result, result1.result);
    }

    @Override
    public int hashCode() {
      return Objects.hash(result);
    }

    @Override
    public String toString() {
      return "Result("
          + result
          .orElse(Optional.of("FAILED"))
          .map(Objects::toString)
          .orElse("null")
          + ')';
    }
  }
}
