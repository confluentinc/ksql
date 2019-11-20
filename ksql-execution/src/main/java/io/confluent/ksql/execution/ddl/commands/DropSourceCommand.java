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
import io.confluent.ksql.name.SourceName;
import java.util.Objects;

@Immutable
public class DropSourceCommand implements DdlCommand {
  private final SourceName sourceName;

  public DropSourceCommand(
      @JsonProperty(value = "sourceName", required = true) SourceName sourceName) {
    this.sourceName = Objects.requireNonNull(sourceName, "sourceName");
  }

  @Override
  public DdlCommandResult execute(Executor executor) {
    return executor.executeDropSource(this);
  }

  public SourceName getSourceName() {
    return sourceName;
  }
}
