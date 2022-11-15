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

package io.confluent.ksql.function;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a list of initial arguments for the creation of a UDAF {@link
 * io.confluent.ksql.function.KsqlAggregateFunction}
 *
 * <p>The initial arguments are always constants.
 */
public class AggregateFunctionInitArguments {

  private final int udafIndex;
  private final List<Object> initArgs;

  public static final AggregateFunctionInitArguments EMPTY_ARGS =
      new AggregateFunctionInitArguments();

  public AggregateFunctionInitArguments(final int index, final Object... initArgs) {
    this(index, Arrays.asList(initArgs));
  }

  public AggregateFunctionInitArguments(final int index, final List<Object> initArgs) {
    this.udafIndex = index;
    this.initArgs = Objects.requireNonNull(initArgs);

    if (index < 0) {
      throw new IllegalArgumentException("index is negative: " + index);
    }
  }

  private AggregateFunctionInitArguments() {
    this.udafIndex = 0;
    this.initArgs = Collections.emptyList();
  }

  public int udafIndex() {
    return udafIndex;
  }

  public Object arg(final int i) {
    return initArgs.get(i);
  }

  public List<Object> args() {
    return initArgs;
  }
}
