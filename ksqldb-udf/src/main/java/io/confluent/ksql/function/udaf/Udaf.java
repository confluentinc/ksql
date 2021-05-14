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

package io.confluent.ksql.function.udaf;

/**
 * {@code Udaf} represents a custom UDAF (User Defined Aggregate Function)
 * that can be used to perform aggregations on KSQL Streams.
 *
 * <p>Type support is presently limited to: int, Integer, long, Long, boolean, Boolean, double,
 * Double, String, Map, and List.
 *
 * <p>Sequence of calls is:
 * <ol>
 *   <li>{@code initialize()}: to get the initial value for the aggregate</li>
 *   <li>{@code aggregate(value, aggregate)}: adds {@code value} to the {@code aggregate}.</li>
 *   <li>{@code merge(agg1, agg2)}: merges to aggregates together, e.g. on session merges.</li>
 *   <li>{@code map(agg)}: reduces the intermediate state to the final output type.</li>
 * </ol>
 *
 * @param <I> the input type
 * @param <A> the intermediate aggregate type
 * @param <O> the final output type
 */
public interface Udaf<I, A, O> {
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
  A aggregate(I current, A aggregate);

  /**
   * Merge two aggregates. Only called when SESSION windows are merged.
   * @param aggOne first aggregate
   * @param aggTwo second aggregate
   * @return new aggregate
   */
  A merge(A aggOne, A aggTwo);

  /**
   * Map the intermediate aggregate value into the actual returned value.
   * @param agg aggregate value of current record
   * @return new value of current record
   */
  O map(A agg);
}
