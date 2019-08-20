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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KTable;

@Immutable
public class TableMapValues<K> implements ExecutionStep<KTable<K, GenericRow>> {
  private final ExecutionStepProperties properties;
  private final ExecutionStep<KTable<K, GenericRow>> source;
  private final List<SelectExpression> selectExpressions;

  public TableMapValues(
      final ExecutionStepProperties properties,
      final ExecutionStep<KTable<K, GenericRow>> source,
      final List<SelectExpression> selectExpressions
  ) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.selectExpressions = selectExpressions;
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
  public KTable<K, GenericRow> build(final StreamsBuilder builder) {
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
    final TableMapValues<?> that = (TableMapValues<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(source, that.source)
        && Objects.equals(selectExpressions, that.selectExpressions);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, selectExpressions);
  }
}
