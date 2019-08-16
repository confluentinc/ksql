/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License; you may not use this file
 * except in compliance with the License.  You may obtain a copy of the License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.execution.plan;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.function.KsqlAggregateFunction;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KTable;

@Immutable
public class StreamAggregate<K> implements ExecutionStep<KTable<K, GenericRow>> {
  private final ExecutionStepProperties properties;
  private final ExecutionStep<KGroupedStream<K, GenericRow>> source;
  private final Formats formats;
  private final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap;
  private final Map<Integer, Integer> aggValToValColumnMap;

  public StreamAggregate(
      final ExecutionStepProperties properties,
      final ExecutionStep<KGroupedStream<K, GenericRow>> source,
      final Formats formats,
      final Map<Integer, KsqlAggregateFunction> aggValToFunctionMap,
      final Map<Integer, Integer> aggValToValColumnMap) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.aggValToFunctionMap = Objects.requireNonNull(aggValToFunctionMap);
    this.aggValToValColumnMap = Objects.requireNonNull(aggValToValColumnMap);
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  @Override
  public KTable<K, GenericRow> build(final StreamsBuilder streamsBuilder) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamAggregate<?> that = (StreamAggregate<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(formats, that.formats)
        && Objects.equals(aggValToFunctionMap, that.aggValToFunctionMap)
        && Objects.equals(aggValToValColumnMap, that.aggValToValColumnMap);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, formats, aggValToFunctionMap, aggValToValColumnMap);
  }
}
