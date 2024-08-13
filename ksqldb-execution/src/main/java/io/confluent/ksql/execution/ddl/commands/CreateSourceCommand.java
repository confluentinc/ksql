/*
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.execution.ddl.commands;

import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlException;
import java.util.Objects;
import java.util.Optional;

/**
 * Base class of create table/stream command
 */
public abstract class CreateSourceCommand implements DdlCommand {

  private final SourceName sourceName;
  private final LogicalSchema schema;
  private final Optional<TimestampColumn> timestampColumn;
  private final String topicName;
  private final Formats formats;
  private final Optional<WindowInfo> windowInfo;
  private final Boolean orReplace;
  private final Boolean isSource;

  CreateSourceCommand(
      final SourceName sourceName,
      final LogicalSchema schema,
      final Optional<TimestampColumn> timestampColumn,
      final String topicName,
      final Formats formats,
      final Optional<WindowInfo> windowInfo,
      final Boolean orReplace,
      final Boolean isSource
  ) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.timestampColumn =
        Objects.requireNonNull(timestampColumn, "timestampColumn");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.windowInfo = Objects.requireNonNull(windowInfo, "windowInfo");
    this.orReplace = orReplace;
    this.isSource = isSource;

    validate(schema, windowInfo.isPresent());
  }

  public SourceName getSourceName() {
    return sourceName;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public Optional<TimestampColumn> getTimestampColumn() {
    return timestampColumn;
  }

  public String getTopicName() {
    return topicName;
  }

  public Formats getFormats() {
    return formats;
  }

  public Optional<WindowInfo> getWindowInfo() {
    return windowInfo;
  }

  public Boolean isOrReplace() {
    return orReplace;
  }

  public Boolean getIsSource() {
    return isSource;
  }

  private static void validate(final LogicalSchema schema, final boolean windowed) {

    if (windowed && schema.key().isEmpty()) {
      throw new KsqlException("Windowed sources require a key column.");
    }
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final CreateSourceCommand that = (CreateSourceCommand) o;
    return Objects.equals(sourceName, that.sourceName)
        && Objects.equals(schema, that.schema)
        && Objects.equals(timestampColumn, that.timestampColumn)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(formats, that.formats)
        && Objects.equals(windowInfo, that.windowInfo)
        && Objects.equals(isSource, that.isSource);
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(sourceName, schema, timestampColumn, topicName, formats, windowInfo, isSource);
  }
}
