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
import io.confluent.ksql.execution.plan.Formats;
import io.confluent.ksql.execution.timestamp.TimestampColumn;
import io.confluent.ksql.name.ColumnName;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.LogicalSchema;
import io.confluent.ksql.serde.WindowInfo;
import java.util.Optional;

@Immutable
public class CreateStreamCommand extends CreateSourceCommand {
  public CreateStreamCommand(
      @JsonProperty(value = "sourceName", required = true) SourceName sourceName,
      @JsonProperty(value = "schema", required = true) LogicalSchema schema,
      @JsonProperty(value = "keyField") Optional<ColumnName> keyField,
      @JsonProperty(value = "timestampColumn")
      Optional<TimestampColumn> timestampColumn,
      @JsonProperty(value = "topicName", required = true) String topicName,
      @JsonProperty(value = "formats", required = true) final Formats formats,
      @JsonProperty(value = "windowInfo") final Optional<WindowInfo> windowInfo
  ) {
    super(
        sourceName,
        schema,
        keyField,
        timestampColumn,
        topicName,
        formats,
        windowInfo
    );
  }

  @Override
  public DdlCommandResult execute(Executor executor) {
    return executor.executeCreateStream(this);
  }
}
