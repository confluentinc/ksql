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
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.rest.server.computation.CommandId;

import java.util.Map;
import java.util.Objects;

@JsonTypeName("currentStatus")
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

  //  Commented-out now due to Jackson issues with deserializing unwrapped values; may be possible
  //  to do things this way once Jackson 2.9.0 comes out but until then have to stick with the
  //  Map<String, Object> constructor hack
  //  @JsonCreator
  //  public CommandStatusEntity(
  //      @JsonProperty("statementText") String statementText,
  //      @JsonProperty("commandId") String commandId,
  //      @JsonProperty("status") String status,
  //      @JsonProperty("message") String message
  //  ) {
  //    this(
  //        statementText,
  //        CommandId.fromString(commandId),
  //        new CommandStatus(CommandStatus.Status.valueOf(status), message)
  //    );
  //  }

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
        (String) properties.get("status"),
        (String) properties.get("message")
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
