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

package io.confluent.ksql.parser.tree;

import org.apache.kafka.streams.kstream.Initializer;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;

import java.util.Optional;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;

public abstract class KsqlWindowExpression extends Node {

  protected KsqlWindowExpression(Optional<NodeLocation> location) {
    super(location);
  }

  public abstract KTable applyAggregate(final KGroupedStream groupedStream,
                                        final Initializer initializer,
                                        final UdafAggregator aggregator,
                                        final Materialized<String, GenericRow, ?> materialized);
}
