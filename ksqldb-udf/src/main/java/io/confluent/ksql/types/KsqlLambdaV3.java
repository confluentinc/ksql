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

import io.confluent.ksql.function.TriFunction;

public class KsqlLambdaV3<T, U, V, R> {

  private final T arg1;
  private final U arg2;
  private final V arg3;
  private final R returnType;
  private TriFunction<T, U, V, R> function;

  public KsqlLambdaV3(
      final T arg1,
      final U arg2,
      final V arg3,
      final R returnType
  ) {
    this.arg1 = arg1;
    this.arg2 = arg2;
    this.arg3 = arg3;
    this.returnType = returnType;
  }

  public KsqlLambdaV3(
      final T arg1,
      final U arg2,
      final V arg3,
      final R returnType,
      final TriFunction<T, U, V, R> function
  ) {
    this.arg1 = arg1;
    this.arg2 = arg2;
    this.arg3 = arg3;
    this.returnType = returnType;
    this.function = function;
  }

  public T getArg1() {
    return arg1;
  }

  public U getArg2() {
    return arg2;
  }
  
  public V getArg3() {
    return arg3;
  }

  public R getReturnType() {
    return returnType;
  }

  public TriFunction<T, U, V, R> getFunction() {
    return function;
  }
}
