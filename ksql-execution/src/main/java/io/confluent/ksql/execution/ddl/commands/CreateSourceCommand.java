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

import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Base class of create table/stream command
 */
public abstract class CreateSourceCommand implements DdlCommand {
  private final SourceName sourceName;
  private final LogicalSchema schema;
  private final Optional<ColumnName> keyField;
  private final Optional<TimestampColumn> timestampColumn;
  private final Set<SerdeOption> serdeOptions;
  private final KsqlTopic topic;

  CreateSourceCommand(
      final SourceName sourceName,
      final LogicalSchema schema,
      final Optional<ColumnName> keyField,
      final Optional<TimestampColumn> timestampColumn,
      final Set<SerdeOption> serdeOptions,
      final KsqlTopic ksqlTopic
  ) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.topic = Objects.requireNonNull(ksqlTopic, "topic");
    this.keyField = Objects.requireNonNull(keyField, "keyField");
    this.timestampColumn =
        Objects.requireNonNull(timestampColumn, "timestampColumn");
    this.serdeOptions = Objects.requireNonNull(serdeOptions, "serdeOptions");
  }

  public Set<SerdeOption> getSerdeOptions() {
    return serdeOptions;
  }

  public KsqlTopic getTopic() {
    return topic;
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
}
