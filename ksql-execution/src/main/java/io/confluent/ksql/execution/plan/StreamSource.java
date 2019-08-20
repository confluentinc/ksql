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
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

@Immutable
public class StreamSource<K> implements ExecutionStep<KStream<K, GenericRow>> {
  private final ExecutionStepProperties properties;
  private final String topicName;
  private final Formats formats;
  private final TimestampExtractionPolicy timestampPolicy;
  private final Topology.AutoOffsetReset offsetReset;

  public StreamSource(
      final ExecutionStepProperties properties,
      final String topicName,
      final Formats formats,
      final TimestampExtractionPolicy timestampPolicy,
      final Topology.AutoOffsetReset offsetReset) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.timestampPolicy = Objects.requireNonNull(timestampPolicy, "timestampPolicy");
    this.offsetReset = Objects.requireNonNull(offsetReset, "offsetReset");
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.emptyList();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamSource<?> that = (StreamSource<?>) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(formats, that.formats)
        && Objects.equals(timestampPolicy, that.timestampPolicy)
        && offsetReset == that.offsetReset;
  }

  @Override
  public int hashCode() {

    return Objects.hash(properties, topicName, formats, timestampPolicy, offsetReset);
  }

  @Override
  public KStream<K, GenericRow> build(final StreamsBuilder builder) {
    throw new UnsupportedOperationException();
  }
}
