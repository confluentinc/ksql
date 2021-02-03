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

package io.confluent.ksql.types;

import java.util.function.BiFunction;

public class KsqlLambdaV2<T, U, R> {

  private final T arg1;
  private final U arg2;
  private final R returnType;
  private BiFunction<T, U, R> function;

  public KsqlLambdaV2(
      final T arg1,
      final U arg2,
      final R returnType
  ) {
    this.arg1 = arg1;
    this.arg2 = arg2;
    this.returnType = returnType;
  }

  public KsqlLambdaV2(
      final T arg1,
      final U arg2,
      final R returnType,
      final BiFunction<T, U, R> function
  ) {
    this.arg1 = arg1;
    this.arg2 = arg2;
    this.returnType = returnType;
    this.function = function;
  }

  public T getArg1() {
    return arg1;
  }

  public U getArg2() {
    return arg2;
  }

  public R getReturnType() {
    return returnType;
  }

  public BiFunction<T, U, R> getFunction() {
    return function;
  }
}
