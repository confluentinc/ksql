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

import com.google.common.annotations.VisibleForTesting;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.builder.KsqlQueryBuilder;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.BiFunction;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

@Immutable
public class StreamSource<S> implements ExecutionStep<S> {
  private final ExecutionStepProperties properties;
  private final String topicName;
  private final Formats formats;
  private final TimestampExtractionPolicy timestampPolicy;
  private final int timestampIndex;
  private final Optional<AutoOffsetReset> offsetReset;
  private final LogicalSchema sourceSchema;
  private final BiFunction<KsqlQueryBuilder, StreamSource<S>, S> builder;

  public static LogicalSchemaWithMetaAndKeyFields getSchemaWithMetaAndKeyFields(
      final String alias,
      final LogicalSchema schema) {
    return LogicalSchemaWithMetaAndKeyFields.fromOriginal(alias, schema);
  }

  @VisibleForTesting
  public StreamSource(
      final ExecutionStepProperties properties,
      final String topicName,
      final Formats formats,
      final TimestampExtractionPolicy timestampPolicy,
      final int timestampIndex,
      final Optional<AutoOffsetReset> offsetReset,
      final LogicalSchema sourceSchema,
      final BiFunction<KsqlQueryBuilder, StreamSource<S>, S> builder) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.timestampPolicy = Objects.requireNonNull(timestampPolicy, "timestampPolicy");
    this.timestampIndex = timestampIndex;
    this.offsetReset = Objects.requireNonNull(offsetReset, "offsetReset");
    this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema");
    this.builder = Objects.requireNonNull(builder, "builder");
  }

  @Override
  public S build(final KsqlQueryBuilder ksqlQueryBuilder) {
    return builder.apply(ksqlQueryBuilder, this);
  }

  @Override
  public ExecutionStepProperties getProperties() {
    return properties;
  }

  @Override
  public List<ExecutionStep<?>> getSources() {
    return Collections.emptyList();
  }

  public LogicalSchema getSourceSchema() {
    return sourceSchema;
  }

  public Optional<AutoOffsetReset> getOffsetReset() {
    return offsetReset;
  }

  public int getTimestampIndex() {
    return timestampIndex;
  }

  public TimestampExtractionPolicy getTimestampPolicy() {
    return timestampPolicy;
  }

  public Formats getFormats() {
    return formats;
  }

  public String getTopicName() {
    return topicName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamSource that = (StreamSource) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(formats, that.formats)
        && Objects.equals(timestampPolicy, that.timestampPolicy);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        properties,
        topicName,
        formats,
        timestampPolicy
    );
  }
}
