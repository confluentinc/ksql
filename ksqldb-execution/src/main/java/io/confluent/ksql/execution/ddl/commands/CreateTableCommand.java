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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.errorprone.annotations.Immutable;
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties({"keyField"}) // Removed at version 0.10
@Immutable
public class CreateTableCommand extends CreateSourceCommand {

  public CreateTableCommand(
      @JsonProperty(value = "sourceName", required = true) final SourceName sourceName,
      @JsonProperty(value = "schema", required = true) final LogicalSchema schema,
      @JsonProperty("timestampColumn") final Optional<TimestampColumn> timestampColumn,
      @JsonProperty(value = "topicName", required = true) final String topicName,
      @JsonProperty(value = "formats", required = true) final Formats formats,
      @JsonProperty(value = "windowInfo") final Optional<WindowInfo> windowInfo,
      @JsonProperty(value = "orReplace", defaultValue = "false") final Optional<Boolean> orReplace,
      @JsonProperty(value = "isSource", defaultValue = "false") final Optional<Boolean> isSource
  ) {
    super(
        sourceName,
        schema,
        timestampColumn,
        topicName,
        formats,
        windowInfo,
        orReplace.orElse(false),
        isSource.orElse(false)
    );

    if (schema.key().isEmpty()) {
      throw new UnsupportedOperationException("Tables require key columns");
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
    final CreateTableCommand that = (CreateTableCommand) o;
    return super.equals(that);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        getSourceName(),
        getSchema(),
        getTimestampColumn(),
        getTopicName(),
        getFormats(),
        getWindowInfo(),
        getIsSource());
  }

  @Override
  public DdlCommandResult execute(final Executor executor) {
    return executor.executeCreateTable(this);
  }
}
