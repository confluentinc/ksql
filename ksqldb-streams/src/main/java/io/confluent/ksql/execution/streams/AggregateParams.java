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

package io.confluent.ksql.execution.streams;

import io.confluent.ksql.execution.function.udaf.KudafAggregator;
import io.confluent.ksql.execution.function.udaf.KudafInitializer;
import io.confluent.ksql.execution.function.udaf.KudafUndoAggregator;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Objects;
import java.util.Optional;

public final class AggregateParams {
  private final KudafInitializer initializer;
  private final KudafAggregator aggregator;
  private final Optional<KudafUndoAggregator> undoAggregator;
  private final LogicalSchema aggregateSchema;
  private final LogicalSchema schema;

  AggregateParams(
      final KudafInitializer initializer,
      final KudafAggregator aggregator,
      final Optional<KudafUndoAggregator> undoAggregator,
      final LogicalSchema aggregateSchema,
      final LogicalSchema schema) {
    this.initializer = Objects.requireNonNull(initializer, "initializer");
    this.aggregator = Objects.requireNonNull(aggregator, "aggregator");
    this.undoAggregator = Objects.requireNonNull(undoAggregator, "undoAggregator");
    this.aggregateSchema = Objects.requireNonNull(aggregateSchema, "aggregateSchema");
    this.schema = Objects.requireNonNull(schema, "schema");
  }

  KudafInitializer getInitializer() {
    return initializer;
  }

  @SuppressWarnings("unchecked")
  <K> KudafAggregator<K> getAggregator() {
    return aggregator;
  }

  Optional<KudafUndoAggregator> getUndoAggregator() {
    return undoAggregator;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public LogicalSchema getAggregateSchema() {
    return aggregateSchema;
  }
}
