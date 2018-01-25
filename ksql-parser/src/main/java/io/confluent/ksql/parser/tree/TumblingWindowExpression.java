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
import org.apache.kafka.streams.kstream.TimeWindows;

import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.UdafAggregator;

public class TumblingWindowExpression extends KsqlWindowExpression {

  private final long size;
  private final TimeUnit sizeUnit;

  public TumblingWindowExpression(long size, TimeUnit sizeUnit) {
    this(Optional.empty(), size, sizeUnit);
  }

  private TumblingWindowExpression(Optional<NodeLocation> location, long size,
                                   TimeUnit sizeUnit) {
    super(location);
    this.size = size;
    this.sizeUnit = sizeUnit;
  }

  @Override
  public String toString() {
    return " TUMBLING ( SIZE " + size + " " + sizeUnit + " ) ";
  }

  @Override
  public int hashCode() {
    return Objects.hash(size, sizeUnit);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TumblingWindowExpression tumblingWindowExpression = (TumblingWindowExpression) o;
    return tumblingWindowExpression.size == size && tumblingWindowExpression.sizeUnit == sizeUnit;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KTable applyAggregate(final KGroupedStream groupedStream,
                               final Initializer initializer,
                               final UdafAggregator aggregator,
                               final Materialized<String, GenericRow, ?> materialized) {
    return groupedStream.windowedBy(TimeWindows.of(sizeUnit.toMillis(size)))
        .aggregate(initializer, aggregator, materialized);

  }
}
