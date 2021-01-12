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

import com.google.common.collect.ImmutableMap;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a list of initial arguments for the creation of a UDAF {@link
 * io.confluent.ksql.function.KsqlAggregateFunction}
 *
 * <p>The initial arguments are always constants.
 */
public class AggregateFunctionInitArguments {

  public static final AggregateFunctionInitArguments EMPTY_ARGS =
      new AggregateFunctionInitArguments();

  private final int udafIndex;
  private final List<Object> initArgs;
  private final Map<String, ?> config;

  /**
   * This method should only be used for legacy "built-in" UDAF
   * implementations that implement AggregateFunctionFactory directly
   * such as TopKAggregateFuncitonFactory. Otherwise, the config will
   * not be properly passed through to the aggregate function.
   */
  public AggregateFunctionInitArguments(
      final int index,
      final Object... initArgs
  ) {
    this(index, ImmutableMap.of(/* not a configurable function */), Arrays.asList(initArgs));
  }

  public AggregateFunctionInitArguments(
      final int index,
      final Map<String, ?> config,
      final Object... initArgs
  ) {
    this(index, config, Arrays.asList(initArgs));
  }

  public AggregateFunctionInitArguments(
      final int index,
      final Map<String, ?> config,
      final List<Object> initArgs
  ) {
    this.udafIndex = index;
    this.config = ImmutableMap.copyOf(Objects.requireNonNull(config, "config"));
    this.initArgs = Objects.requireNonNull(initArgs);

    if (index < 0) {
      throw new IllegalArgumentException("index is negative: " + index);
    }
  }

  private AggregateFunctionInitArguments() {
    this.udafIndex = 0;
    this.config = ImmutableMap.of();
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

  public Map<String, ?> config() {
    return config;
  }
}
