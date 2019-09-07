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

import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Base class of create table/stream command
 */
abstract class CreateSourceCommand implements DdlCommand {
  final static String SQL_EXPRESSION = "sqlExpression";
  final static String SOURCE_NAME = "sourceName";
  final static String SCHEMA = "schema";
  final static String KEY_FIELD = "keyField";
  final static String TIMESTAMP_EXTRACTION_POLICY = "timestampExtractionPolicy";
  final static String SERDE_OPTIONS = "serdeOptions";
  final static String TOPIC = "topic";

  @JsonProperty(SQL_EXPRESSION)
  private final String sqlExpression;
  @JsonProperty(SOURCE_NAME)
  private final String sourceName;
  @JsonProperty(SCHEMA)
  private final LogicalSchema schema;
  @JsonProperty(KEY_FIELD)
  private final Optional<String> keyField;
  @JsonProperty(TIMESTAMP_EXTRACTION_POLICY)
  private final TimestampExtractionPolicy timestampExtractionPolicy;
  @JsonProperty(SERDE_OPTIONS)
  private final Set<SerdeOption> serdeOptions;
  @JsonProperty(TOPIC)
  private final KsqlTopic topic;

  CreateSourceCommand(
      final String sqlExpression,
      final String sourceName,
      final LogicalSchema schema,
      final Optional<String> keyField,
      final TimestampExtractionPolicy timestampExtractionPolicy,
      final Set<SerdeOption> serdeOptions,
      final KsqlTopic ksqlTopic
  ) {
    this.sqlExpression = Objects.requireNonNull(sqlExpression, "sqlExpression");
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

  public String getSqlExpression() {
    return sqlExpression;
  }

  public String getSourceName() {
    return sourceName;
  }

  public LogicalSchema getSchema() {
    return schema;
  }

  public TimestampExtractionPolicy getTimestampExtractionPolicy() {
    return timestampExtractionPolicy;
  }

  public Optional<String> getKeyField() {
    return keyField;
  }
}
