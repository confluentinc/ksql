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

package io.confluent.ksql.function.udtf;

import java.util.List;

/**
 * {@code Udtf} represents a custom UDTF (User Defined Table Function)
 * that can be used to explode a row into zero or more resulting rows on KSQL Streams.
 *
 * <p>Type support is presently limited to Array
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
public interface Udtf<I, A, O> {

  /**
   * Aggregates the current value into the existing aggregate
   * @param current  value from the current record
   * @param aggregate value of the Aggregate
   * @return new aggregate
   */
  List<A> explode(I current, A aggregate);

  /**
   * Merge two aggregates
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
