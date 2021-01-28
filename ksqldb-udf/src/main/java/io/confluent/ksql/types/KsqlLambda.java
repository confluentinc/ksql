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

import java.util.function.Function;

public class KsqlLambda<T, R> {
  
  private final T arg1;
  private final R returnType;
  private Function<T, R> function;

  public KsqlLambda(
      final T arg1,
      final R returnType
  ) {
    this.arg1 = arg1;
    this.returnType = returnType;
  }

  public KsqlLambda(
      final T arg1,
      final R returnType,
      final Function<T, R> function
  ) {
    this.arg1 = arg1;
    this.returnType = returnType;
    this.function = function;
  }

  public T getArg1() {
    return arg1;
  }

  public R getReturnType() {
    return returnType;
  }

  public Function<T, R> getFunction() {
    return function;
  }
}
