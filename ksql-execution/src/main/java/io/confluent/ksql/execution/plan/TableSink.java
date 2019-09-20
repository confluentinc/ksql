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

import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.GenericRow;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.kstream.KTable;

@Immutable
public class TableSink<K> implements ExecutionStep<KTable<K, GenericRow>> {
  private final ExecutionStepProperties properties;
  private final ExecutionStep<KTable<K, GenericRow>> source;
  private final Formats formats;
  private final String topicName;

  public TableSink(
      final ExecutionStepProperties properties,
      final ExecutionStep<KTable<K, GenericRow>> source,
      final Formats formats,
      final String topicName
  ) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.source = Objects.requireNonNull(source, "source");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.singletonList(source);
  }

  public Formats getFormats() {
    return formats;
  }

  @Override
  public KTable<K, GenericRow> build(final KsqlQueryBuilder builder) {
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
    final TableSink<?> tableSink = (TableSink<?>) o;
    return Objects.equals(properties, tableSink.properties)
        && Objects.equals(source, tableSink.source)
        && Objects.equals(formats, tableSink.formats)
        && Objects.equals(topicName, tableSink.topicName);
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, source, formats, topicName);
  }
}
