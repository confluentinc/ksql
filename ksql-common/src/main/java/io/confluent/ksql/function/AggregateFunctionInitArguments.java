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

import io.confluent.ksql.util.KsqlException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a list of initial arguments for the creation of a UDAF
 * {@link io.confluent.ksql.function.KsqlAggregateFunction}
 *
 * <p>The initial arguments are always constants.
 */
public class AggregateFunctionInitArguments {

  private final int udafIndex;
  private final List<String> initArgs;

  public static final AggregateFunctionInitArguments EMPTY_ARGS =
      new AggregateFunctionInitArguments();

  private AggregateFunctionInitArguments() {
    this.udafIndex = 0;
    this.initArgs = Collections.emptyList();
  }

  public static AggregateFunctionInitArguments ofFunctionArgs(final int index,
      final List<String> initArgs) {
    /*
    The first argument to an aggregate function is the value being aggregated, the
    arguments after that are the actual init arguments (constants).
    So we remove the first argument as it's not an init argument.
    The args can also be empty (e.g. in the case of COUNT(*)
    */
    return new AggregateFunctionInitArguments(index, Objects.requireNonNull(
        initArgs.size() < 2 ? Collections.emptyList() : initArgs.subList(1, initArgs.size())
    ));
  }

  public AggregateFunctionInitArguments(final int index, final String... initArgs) {
    this(index, Arrays.asList(initArgs));
  }

  private AggregateFunctionInitArguments(final int index, final List<String> initArgs) {
    this.udafIndex = index;
    this.initArgs = Objects.requireNonNull(initArgs);

    if (index < 0) {
      throw new IllegalArgumentException("index is negative: " + index);
    }
  }

  public int udafIndex() {
    return udafIndex;
  }

  public String arg(final int i) {
    return initArgs.get(i);
  }

  public void ensureArgCount(final int expectedCount, final String functionName) {
    if (initArgs.size() != expectedCount) {
      throw new KsqlException(
          String.format("Invalid parameter count for %s. Need %d args, got %d arg(s)",
              functionName, expectedCount, initArgs.size()));
    }
  }

  public int argsSize() {
    return initArgs.size();
  }

}
