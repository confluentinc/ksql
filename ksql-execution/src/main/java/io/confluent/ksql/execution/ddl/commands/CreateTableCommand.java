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
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.SerdeOption;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import java.util.Set;

@Immutable
public class CreateTableCommand extends CreateSourceCommand {
  public CreateTableCommand(
      @JsonProperty(value = "sqlExpression", required = true) final String sqlExpression,
      @JsonProperty(value = "sourceName", required = true) final SourceName sourceName,
      @JsonProperty(value = "schema", required = true) final LogicalSchema schema,
      @JsonProperty(value = "keyField", required = true) final Optional<ColumnName> keyField,
      @JsonProperty(value = "timestampExtractionPolicy", required = true)
      final TimestampExtractionPolicy extractionPolicy,
      @JsonProperty(value = "serdeOptions", required = true) final Set<SerdeOption> serdeOptions,
      @JsonProperty(value = "topic", required = true) final KsqlTopic ksqlTopic
  ) {
    super(
        sqlExpression,
        sourceName,
        schema,
        keyField,
        extractionPolicy,
        serdeOptions,
        ksqlTopic
    );
  }

  @Override
  public DdlCommandResult execute(final Executor executor) {
    return executor.executeCreateTable(this);
  }
}
