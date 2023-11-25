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
import java.util.Optional;

@JsonIgnoreProperties({"keyField"}) // Removed after version 0.9
@Immutable
public class CreateStreamCommand extends CreateSourceCommand {

  public CreateStreamCommand(
      @JsonProperty(value = "sourceName", required = true) final SourceName sourceName,
      @JsonProperty(value = "schema", required = true) final LogicalSchema schema,
      @JsonProperty(value = "timestampColumn") final
      Optional<TimestampColumn> timestampColumn,
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
  }

  @Override
  public DdlCommandResult execute(final Executor executor) {
    return executor.executeCreateStream(this);
  }
}
