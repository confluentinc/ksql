/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;

public abstract class BaseComparableKudaf<T extends Comparable<? super T>> implements
    Udaf<T, T, T> {

  private final BiFunction<T, T, T> aggregatePrimitive;
  private SqlType inputSchema;

  public BaseComparableKudaf(final BiFunction<T, T, T> aggregatePrimitive) {
    this.aggregatePrimitive = Objects.requireNonNull(aggregatePrimitive, "aggregatePrimitive");
  }

  @Override
  public void initializeTypeArguments(final List<SqlArgument> argTypeList) {
    inputSchema = argTypeList.get(0).getSqlTypeOrThrow();
  }

  @Override
  public Optional<SqlType> getAggregateSqlType() {
    return Optional.of(inputSchema);
  }

  @Override
  public Optional<SqlType> getReturnSqlType() {
    return Optional.of(inputSchema);
  }

  @Override
  public T initialize() {
    return null;
  }

  @Override
  public T aggregate(final T current, final T aggregate) {
    if (current == null) {
      return aggregate;
    }

    if (aggregate == null) {
      return current;
    }

    return aggregatePrimitive.apply(current, aggregate);
  }

  @Override
  public T merge(final T aggOne, final T aggTwo) {
    return aggregate(aggOne, aggTwo);
  }

  @Override
  public T map(final T agg) {
    return agg;
  }
}
