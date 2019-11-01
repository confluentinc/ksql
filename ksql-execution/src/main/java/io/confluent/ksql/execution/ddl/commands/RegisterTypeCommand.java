/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import io.confluent.ksql.schema.ksql.types.SqlType;
import java.util.Objects;

@Immutable
public class RegisterTypeCommand implements DdlCommand {
  private final SqlType type;
  private final String name;

  public RegisterTypeCommand(
      @JsonProperty(value = "type", required = true) final SqlType type,
      @JsonProperty(value = "name", required = true) final String name) {
    this.type = Objects.requireNonNull(type, "type");
    this.name = Objects.requireNonNull(name, "name");
  }

  @Override
  public DdlCommandResult execute(final Executor executor) {
    return executor.executeRegisterType(this);
  }

  public SqlType getType() {
    return type;
  }

  public String getName() {
    return name;
  }
}
