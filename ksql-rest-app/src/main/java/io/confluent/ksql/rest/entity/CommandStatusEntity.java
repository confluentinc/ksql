package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.rest.server.computation.CommandId;

import java.util.Objects;

public class CommandStatusEntity extends KSQLEntity {
  private final CommandStatus commandStatus;
  private final CommandId commandId;

  @JsonCreator
  public CommandStatusEntity(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("commandStatus") CommandStatus commandStatus,
      @JsonProperty("commandId") CommandId commandId
  ) {
    super(statementText);
    this.commandStatus = commandStatus;
    this.commandId = commandId;
  }

  public CommandStatus getCommandStatus() {
    return commandStatus;
  }

  public CommandId getCommandId() {
    return commandId;
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
    return Objects.equals(getCommandStatus(), that.getCommandStatus()) &&
        Objects.equals(getCommandId(), that.getCommandId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCommandStatus(), getCommandId());
  }
}
