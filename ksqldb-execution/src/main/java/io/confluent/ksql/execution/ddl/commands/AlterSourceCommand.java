/*
 * Copyright 2020 Confluent Inc.
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
import com.google.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.ksql.name.SourceName;
import io.confluent.ksql.schema.ksql.Column;
import java.util.List;

public class AlterSourceCommand implements DdlCommand {
  private final SourceName sourceName;
  private final String ksqlType;
  private final List<Column> newColumns;


  public AlterSourceCommand(
      @JsonProperty(value = "sourceName", required = true) final SourceName sourceName,
      @JsonProperty(value = "ksqlType", required = true) final String ksqlType,
      @JsonProperty(value = "newColumns", required = true) final List<Column> newColumns
  ) {
    this.sourceName = sourceName;
    this.ksqlType = ksqlType;
    this.newColumns = ImmutableList.copyOf(newColumns);
  }

  public SourceName getSourceName() {
    return sourceName;
  }

  public String getKsqlType() {
    return ksqlType;
  }

  @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "newColumns is ImmutableList")
  public List<Column> getNewColumns() {
    return newColumns;
  }

  @Override
  public DdlCommandResult execute(final Executor executor) {
    return executor.executeAlterSource(this);
  }
}
