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

package io.confluent.ksql.execution.codegen.helpers;

import io.confluent.ksql.util.KsqlException;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;

public final class SearchedCaseFunction {

  private SearchedCaseFunction() {

  }

  public static <T> T searchedCaseFunction(
      final List<LazyWhenClause<T>> whenClauses, final Supplier<T> defaultValue
  ) {
    if (whenClauses.isEmpty()) {
      throw new KsqlException("When clause cannot be empty.");
    }

    final Optional<Optional<T>> found = whenClauses.stream()
        .filter(clause -> clause.operand.get())
        .map(clause -> Optional.ofNullable(clause.result.get()))
        .findFirst();

    if (found.isPresent()) {
      return found.get().orElse(null);
    }

    return defaultValue.get();
  }

  public static <T> LazyWhenClause<T> whenClause(
      final Supplier<Boolean> operand,
      final Supplier<T> result
  ) {
    return new LazyWhenClause<>(operand, result);
  }

  public static final class LazyWhenClause<T> {

    private final Supplier<Boolean> operand;
    private final Supplier<T> result;

    private LazyWhenClause(final Supplier<Boolean> operand, final Supplier<T> result) {
      this.operand = Objects.requireNonNull(operand, "operand");
      this.result = Objects.requireNonNull(result, "result");
    }
  }

}
