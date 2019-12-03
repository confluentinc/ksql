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
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.SchemaUtil;
import java.util.Objects;
import java.util.Optional;

/**
 * Base class of create table/stream command
 */
public abstract class CreateSourceCommand implements DdlCommand {
  private final SourceName sourceName;
  private final LogicalSchema schema;
  private final Optional<ColumnName> keyField;
  private final Optional<TimestampColumn> timestampColumn;
  private final String kafkaTopicName;
  private final Formats formats;
  private final Optional<WindowInfo> windowInfo;

  CreateSourceCommand(
      final SourceName sourceName,
      final LogicalSchema schema,
      final Optional<ColumnName> keyField,
      final Optional<TimestampColumn> timestampColumn,
      final String kafkaTopicName,
      final Formats formats,
      final Optional<WindowInfo> windowInfo
  ) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.keyField = Objects.requireNonNull(keyField, "keyField");
    this.timestampColumn =
        Objects.requireNonNull(timestampColumn, "timestampColumn");
    this.kafkaTopicName = Objects.requireNonNull(kafkaTopicName, "kafkaTopicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.windowInfo = Objects.requireNonNull(windowInfo, "windowInfo");

    if (schema.findValueColumn(ColumnRef.withoutSource(SchemaUtil.ROWKEY_NAME)).isPresent()
        || schema.findValueColumn(ColumnRef.withoutSource(SchemaUtil.ROWTIME_NAME)).isPresent()) {
      throw new IllegalArgumentException("Schema contains implicit columns in value schema");
    }
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

  public Optional<ColumnName> getKeyField() {
    return keyField;
  }

  public String getKafkaTopicName() {
    return kafkaTopicName;
  }

  public Formats getFormats() {
    return formats;
  }

  public Optional<WindowInfo> getWindowInfo() {
    return windowInfo;
  }
}
