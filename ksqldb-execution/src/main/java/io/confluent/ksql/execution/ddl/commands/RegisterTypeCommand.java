/*
 * Copyright 2021 Confluent Inc.
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
  private final String typeName;

  public RegisterTypeCommand(
      @JsonProperty(value = "type", required = true) final SqlType type,
      @JsonProperty(value = "typeName", required = true) final String typeName) {
    this.type = Objects.requireNonNull(type, "type");
    this.typeName = Objects.requireNonNull(typeName, "typeName");
  }

  @Override
  public DdlCommandResult execute(final Executor executor) {
    return executor.executeRegisterType(this);
  }

  public SqlType getType() {
    return type;
  }

  public String getTypeName() {
    return typeName;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final RegisterTypeCommand that = (RegisterTypeCommand) o;
    return Objects.equals(type, that.type)
        && Objects.equals(typeName, that.typeName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, typeName);
  }
}
