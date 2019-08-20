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

import java.util.List;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;


public interface KsqlAggregateFunction<V, A> extends IndexedFunction {

  KsqlAggregateFunction<V, A> getInstance(AggregateFunctionArguments aggregateFunctionArguments);

  Supplier<A> getInitialValueSupplier();

  int getArgIndexInValue();

  Schema getReturnType();

  boolean hasSameArgTypes(List<Schema> argTypeList);

  /**
   * Merges values inside the window.
   * @return A - type of return value
   */
  A aggregate(V currentValue, A aggregateValue);

  /**
   * Merges two session windows together with the same merge key.
   */
  Merger<Struct, A> getMerger();

  String getDescription();

  @Override
  default boolean isVariadic() {
    return false;
  }
}
