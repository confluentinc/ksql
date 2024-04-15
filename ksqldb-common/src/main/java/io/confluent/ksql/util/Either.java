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

package io.confluent.ksql.util;

import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Function;

public final class Either<L, R> {

  public static <L, R> Either<L, R> left(L value) {
    return new Either<>(Optional.of(value), Optional.empty());
  }

  public static <L, R> Either<L, R> right(R value) {
    return new Either<>(Optional.empty(), Optional.of(value));
  }

  private final Optional<L> left;
  private final Optional<R> right;

  private Either(Optional<L> l, Optional<R> r) {
    left = l;
    right = r;
  }

  public <T> T map(
      Function<? super L, ? extends T> leftFunction,
      Function<? super R, ? extends T> rightFunction
  ) {
    return left.<T>map(leftFunction).orElseGet(() -> right.map(rightFunction).get());
  }

  public <T> Either<T, R> mapLeft(Function<? super L, ? extends T> lFunc) {
    return new Either<>(left.map(lFunc), right);
  }

  public <T> Either<L, T> mapRight(Function<? super R, ? extends T> rFunc) {
    return new Either<>(left, right.map(rFunc));
  }

  public void apply(Consumer<? super L> lFunc, Consumer<? super R> rFunc) {
    left.ifPresent(lFunc);
    right.ifPresent(rFunc);
  }

  public Optional<L> getLeft() {
    return left;
  }

  public Optional<R> getRight() {
    return right;
  }
}