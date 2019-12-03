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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import org.apache.kafka.streams.Topology.AutoOffsetReset;

@Immutable
public abstract class AbstractStreamSource<K> implements ExecutionStep<K> {
  private final ExecutionStepPropertiesV1 properties;
  private final String topicName;
  private final Formats formats;
  private final Optional<TimestampColumn> timestampColumn;
  private final Optional<AutoOffsetReset> offsetReset;
  private final LogicalSchema sourceSchema;
  private final SourceName alias;

  @VisibleForTesting
  public AbstractStreamSource(
      ExecutionStepPropertiesV1 properties,
      String topicName,
      Formats formats,
      Optional<TimestampColumn> timestampColumn,
      Optional<AutoOffsetReset> offsetReset,
      LogicalSchema sourceSchema,
      SourceName alias) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.timestampColumn = Objects.requireNonNull(timestampColumn, "timestampColumn");
    this.offsetReset = Objects.requireNonNull(offsetReset, "offsetReset");
    this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema");
    this.alias = Objects.requireNonNull(alias, "alias");
  }

  @Override
  public ExecutionStepPropertiesV1 getProperties() {
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

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
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
        && Objects.equals(timestampColumn, that.timestampColumn)
        && Objects.equals(offsetReset, that.offsetReset)
        && Objects.equals(sourceSchema, that.sourceSchema);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        properties,
        topicName,
        formats,
        timestampColumn,
        offsetReset,
        sourceSchema
    );
  }
}
