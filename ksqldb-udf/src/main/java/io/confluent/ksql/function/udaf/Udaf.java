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

package io.confluent.ksql.function.udaf;

import io.confluent.ksql.schema.ksql.SqlArgument;
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.List;
import java.util.Optional;

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

  /**
   * Some UDAFs can operate on any type.  In that case, the UDAF needs to be aware of the column
   * type being used.  This method is called once when the UDAF is being created.
   * Implementors may need to hold on to argument types or compute some other state to be re-used
   * in methods like {@code aggregate} or {@code merge}.
   * @param argTypeList The list of SqlArgument that this UDAF will receive as input.
   */
  default void initializeTypeArguments(List<SqlArgument> argTypeList) { }

  /**
   * Most UDAFs advertise their aggregate type statically via the Java type signature or
   * annotations.
   * For polymorphic UDAFs, implement this method to return the aggregate SQL Type.
   * @return The aggregate SQL Type
   */
  default Optional<SqlType> getAggregateSqlType() {
    return Optional.empty();
  }

  /**
   * Most UDAFs advertise their return type statically via the Java type signature or
   * annotations.
   * For polymorphic UDAFs, implement this method to return the return SQL Type.
   * @return The return SQL Type
   */
  default Optional<SqlType> getReturnSqlType() {
    return Optional.empty();
  }
}
