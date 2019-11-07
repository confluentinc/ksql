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

import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
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
  private final TimestampExtractionPolicy timestampExtractionPolicy;
  private final Set<SerdeOption> serdeOptions;
  private final KsqlTopic topic;

  CreateSourceCommand(
      final SourceName sourceName,
      final LogicalSchema schema,
      final Optional<ColumnName> keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final Set<SerdeOption> serdeOptions,
      final KsqlTopic ksqlTopic
  ) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
    this.schema = Objects.requireNonNull(schema, "schema");
    this.topic = Objects.requireNonNull(ksqlTopic, "topic");
    this.keyField = Objects.requireNonNull(keyField, "keyField");
    this.timestampExtractionPolicy =
        Objects.requireNonNull(timestampExtractionPolicy, "timestampExtractionPolicy");
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

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  public Optional<ColumnName> getKeyField() {
    return keyField;
  }
}
