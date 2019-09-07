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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import java.util.Set;

@Immutable
public class CreateTableCommand extends CreateSourceCommand {
  @JsonCreator
  public CreateTableCommand(
      @JsonProperty(SQL_EXPRESSION) final String sqlExpression,
      @JsonProperty(SOURCE_NAME) final String sourceName,
      @JsonProperty(SCHEMA) final LogicalSchema schema,
      @JsonProperty(KEY_FIELD) final Optional<String> keyField,
      @JsonProperty(TIMESTAMP_EXTRACTION_POLICY) final TimestampExtractionPolicy timestampExtractionPolicy,
      @JsonProperty(SERDE_OPTIONS) final Set<SerdeOption> serdeOptions,
      @JsonProperty(TOPIC) final KsqlTopic ksqlTopic
  ) {
    super(
        sqlExpression,
        sourceName,
        schema,
        keyField,
        timestampExtractionPolicy,
        serdeOptions,
        ksqlTopic
    );
  }

  @Override
  public DdlCommandResult execute(final Executor executor) {
    return executor.executeCreateTable(this);
  }
}
