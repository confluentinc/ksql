/*
 * Copyright 2019 Confluent Inc.
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

package io.confluent.ksql.execution.plan;

import com.google.common.annotations.VisibleForTesting;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

public abstract class AbstractStreamSource<K> implements ExecutionStep<K> {
  private final ExecutionStepProperties properties;
  private final String topicName;
  private final Formats formats;
  private final TimestampExtractionPolicy timestampPolicy;
  private final int timestampIndex;
  private final Optional<AutoOffsetReset> offsetReset;
  private final LogicalSchema sourceSchema;
  private final SourceName alias;

  public static LogicalSchemaWithMetaAndKeyFields getSchemaWithMetaAndKeyFields(
      SourceName alias,
      LogicalSchema schema) {
    return LogicalSchemaWithMetaAndKeyFields.fromOriginal(alias, schema);
  }

  @VisibleForTesting
  public AbstractStreamSource(
      ExecutionStepProperties properties,
      String topicName,
      Formats formats,
      TimestampExtractionPolicy timestampPolicy,
      int timestampIndex,
      Optional<AutoOffsetReset> offsetReset,
      LogicalSchema sourceSchema,
      SourceName alias) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.timestampPolicy = Objects.requireNonNull(timestampPolicy, "timestampPolicy");
    this.timestampIndex = timestampIndex;
    this.offsetReset = Objects.requireNonNull(offsetReset, "offsetReset");
    this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema");
    this.alias = Objects.requireNonNull(alias, "alias");
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

  public SourceName getAlias() {
    return alias;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AbstractStreamSource that = (AbstractStreamSource) o;
    return Objects.equals(properties, that.properties)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(formats, that.formats)
        && Objects.equals(timestampPolicy, that.timestampPolicy)
        && Objects.equals(timestampIndex, that.timestampIndex)
        && Objects.equals(offsetReset, that.offsetReset)
        && Objects.equals(sourceSchema, that.sourceSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        properties,
        topicName,
        formats,
        timestampPolicy,
        timestampIndex,
        offsetReset,
        sourceSchema
    );
  }
}
