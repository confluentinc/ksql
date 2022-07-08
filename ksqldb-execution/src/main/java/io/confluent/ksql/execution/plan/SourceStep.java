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
import io.confluent.ksql.schema.ksql.LogicalSchema;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@Immutable
public abstract class SourceStep<K> implements ExecutionStep<K> {

  final ExecutionStepPropertiesV1 properties;
  final String topicName;
  final Formats formats;
  final Optional<TimestampColumn> timestampColumn;
  final LogicalSchema sourceSchema;
  final int pseudoColumnVersion;

  @VisibleForTesting
  public SourceStep(
      final ExecutionStepPropertiesV1 properties,
      final String topicName,
      final Formats formats,
      final Optional<TimestampColumn> timestampColumn,
      final LogicalSchema sourceSchema,
      final int pseudoColumnVersion
  ) {
    this.properties = Objects.requireNonNull(properties, "properties");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.timestampColumn = Objects.requireNonNull(timestampColumn, "timestampColumn");
    this.sourceSchema = Objects.requireNonNull(sourceSchema, "sourceSchema");
    this.pseudoColumnVersion = pseudoColumnVersion;
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

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
  }

  public Formats getFormats() {
    return formats;
  }

  public String getTopicName() {
    return topicName;
  }

  public int getPseudoColumnVersion() {
    return pseudoColumnVersion;
  }
}
