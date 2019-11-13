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

import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.streams.kstream.Merger;

public interface KsqlAggregateFunction<I, A, O> extends FunctionSignature {

  Supplier<A> getInitialValueSupplier();

  int getArgIndexInValue();

  SqlType getAggregateType();

  SqlType returnType();

  /**
   * Merges values inside the window.
   *
   * @return A - type of return value
   */
  A aggregate(I currentValue, A aggregateValue);

  /**
   * Merges two session windows together with the same merge key.
   */
  Merger<Struct, A> getMerger();

  Function<A, O> getResultMapper();

  String getDescription();
}
