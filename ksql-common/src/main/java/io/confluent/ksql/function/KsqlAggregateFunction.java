/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.function;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.kstream.Merger;

import java.util.List;
import java.util.function.Supplier;


public interface KsqlAggregateFunction<V, A> {

  KsqlAggregateFunction<V, A> getInstance(AggregateFunctionArguments aggregateFunctionArguments);

  String getFunctionName();

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
  Merger<String, A> getMerger();

  List<Schema> getArgTypes();

  String getDescription();
}
