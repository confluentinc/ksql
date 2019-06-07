/*
 * Copyright 2018 Confluent Inc.
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
 */

package io.confluent.ksql.function.udaf;

/**
 * {@code Udaf} represents a custom UDAF (User Defined Aggregate Function)
 * that can be used to perform aggregations on KSQL Streams.
 * Type support is presently limited to: int, Integer, long, Long, boolean, Boolean, double,
 * Double, String, Map, and List.
 *
 * @param <V> value type
 * @param <A> aggregate type
 */
public interface Udaf<V, A> {
  /**
   * The initializer for the Aggregation
   * @return initial value to use when aggregating
   */
  A initialize();

  /**
   * Aggregates the current value into the existing aggregate
   * @param current  value from the current record
   * @param aggregate value of the Aggregate
   * @return new aggregate
   */
  A aggregate(final V current, final A aggregate);

  /**
   * Merge two aggregates
   * @param aggOne first aggregate
   * @param aggTwo second aggregate
   * @return new aggregate
   */
  A merge(final A aggOne, final A aggTwo);
}
