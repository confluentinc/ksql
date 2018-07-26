/*
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Map;
import java.util.Objects;

import io.confluent.ksql.rest.server.computation.CommandId;

@JsonIgnoreProperties(ignoreUnknown = true)
public class CommandStatusEntity extends KsqlEntity {
  private final CommandId commandId;
  private final CommandStatus commandStatus;

  public CommandStatusEntity(
      final String statementText,
      final CommandId commandId,
      final CommandStatus commandStatus
  ) {
    super(statementText);
    this.commandId = commandId;
    this.commandStatus = commandStatus;
  }

  public CommandStatusEntity(
      final String statementText,
      final String commandId,
      final String status,
      final String message
  ) {
    this(
        statementText,
        CommandId.fromString(commandId),
        new CommandStatus(CommandStatus.Status.valueOf(status), message)
    );
  }

  @SuppressWarnings("unchecked") // needs investigating
  @JsonCreator
  public CommandStatusEntity(final Map<String, Object> properties) {
    this(
        (String) properties.get("statementText"),
        (String) properties.get("commandId"),
        (String) ((Map<String, Object>) properties.get("commandStatus")).get("status"),
        (String) ((Map<String, Object>) properties.get("commandStatus")).get("message")
    );
  }

  public CommandId getCommandId() {
    return commandId;
  }

  @JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
  public CommandStatus getCommandStatus() {
    return commandStatus;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandStatusEntity)) {
      return false;
    }
    final CommandStatusEntity that = (CommandStatusEntity) o;
    return Objects.equals(getCommandId(), that.getCommandId())
        && Objects.equals(getCommandStatus(), that.getCommandStatus());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCommandId(), getCommandStatus());
  }
}
