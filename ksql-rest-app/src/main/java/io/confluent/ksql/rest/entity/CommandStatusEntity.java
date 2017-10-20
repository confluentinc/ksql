/**
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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.rest.server.computation.CommandId;

import java.util.Map;
import java.util.Objects;

@JsonTypeName("currentStatus")
@JsonSubTypes({})
public class CommandStatusEntity extends KsqlEntity {
  private final CommandId commandId;
  private final CommandStatus commandStatus;

  public CommandStatusEntity(
      String statementText,
      CommandId commandId,
      CommandStatus commandStatus
  ) {
    super(statementText);
    this.commandId = commandId;
    this.commandStatus = commandStatus;
  }

  public CommandStatusEntity(
      String statementText,
      String commandId,
      String status,
      String message
  ) {
    this(
        statementText,
        CommandId.fromString(commandId),
        new CommandStatus(CommandStatus.Status.valueOf(status), message)
    );
  }

  @JsonCreator
  public CommandStatusEntity(Map<String, Object> properties) {
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
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandStatusEntity)) {
      return false;
    }
    CommandStatusEntity that = (CommandStatusEntity) o;
    return Objects.equals(getCommandId(), that.getCommandId())
        && Objects.equals(getCommandStatus(), that.getCommandStatus());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCommandId(), getCommandStatus());
  }
}
