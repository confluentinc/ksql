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
import io.confluent.ksql.schema.ksql.Column;
import io.confluent.ksql.schema.ksql.ColumnRef;
import io.confluent.ksql.schema.ksql.FormatOptions;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.schema.ksql.types.SqlType;
import io.confluent.ksql.serde.WindowInfo;
import io.confluent.ksql.util.KsqlException;
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
  private final String topicName;
  private final Formats formats;
  private final Optional<WindowInfo> windowInfo;

  CreateSourceCommand(
      final SourceName sourceName,
      final LogicalSchema schema,
      final Optional<ColumnName> keyField,
      final Optional<TimestampColumn> timestampColumn,
      final String topicName,
      final Formats formats,
      final Optional<WindowInfo> windowInfo
  ) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.keyField = Objects.requireNonNull(keyField, "keyField");
    this.timestampColumn =
        Objects.requireNonNull(timestampColumn, "timestampColumn");
    this.topicName = Objects.requireNonNull(topicName, "topicName");
    this.formats = Objects.requireNonNull(formats, "formats");
    this.windowInfo = Objects.requireNonNull(windowInfo, "windowInfo");

    validate(schema, keyField);
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

  public String getTopicName() {
    return topicName;
  }

  public Formats getFormats() {
    return formats;
  }

  public Optional<WindowInfo> getWindowInfo() {
    return windowInfo;
  }

  private static void validate(final LogicalSchema schema, final Optional<ColumnName> keyField) {
    if (schema.findValueColumn(ColumnRef.withoutSource(SchemaUtil.ROWKEY_NAME)).isPresent()
        || schema.findValueColumn(ColumnRef.withoutSource(SchemaUtil.ROWTIME_NAME)).isPresent()) {
      throw new IllegalArgumentException("Schema contains implicit columns in value schema");
    }

    if (schema.key().size() != 1) {
      throw new UnsupportedOperationException("Only single key columns supported");
    }

    if (keyField.isPresent()) {
      final SqlType keyFieldType = schema.findColumn(ColumnRef.withoutSource(keyField.get()))
          .map(Column::type)
          .orElseThrow(IllegalArgumentException::new);

      final SqlType keyType = schema.key().get(0).type();

      if (!keyFieldType.equals(keyType)) {
        throw new KsqlException("The KEY field ("
            + keyField.get().toString(FormatOptions.noEscape())
            + ") identified in the WITH clause is of a different type to the actual key column."
            + System.lineSeparator()
            + "Either change the type of the KEY field to match ROWKEY, "
            + "or explicitly set ROWKEY to the type of the KEY field by adding "
            + "'ROWKEY " + keyFieldType + " KEY' in the schema."
            + System.lineSeparator()
            + "KEY field type: " + keyFieldType
            + System.lineSeparator()
            + "ROWKEY type: " + keyType
        );
      }
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
        && Objects.equals(keyField, that.keyField)
        && Objects.equals(timestampColumn, that.timestampColumn)
        && Objects.equals(topicName, that.topicName)
        && Objects.equals(formats, that.formats)
        && Objects.equals(windowInfo, that.windowInfo);
  }

  @Override
  public int hashCode() {
    return Objects
        .hash(sourceName, schema, keyField, timestampColumn, topicName, formats, windowInfo);
  }
}
