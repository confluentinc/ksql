/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.rest.server.computation.CommandId;

import java.util.Objects;

@JsonTypeName("commandId")
public class CommandIdEntity extends KSQLEntity {

  private final CommandId commandId;

  public CommandIdEntity(String statementText, CommandId commandId) {
    super(statementText);
    this.commandId = commandId;
  }

  @JsonCreator
  public CommandIdEntity(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("commandId")     String commandId
  ) {
    this(statementText, CommandId.fromString(commandId));
  }

  @JsonUnwrapped
  public CommandId getCommandId() {
    return commandId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandIdEntity)) {
      return false;
    }
    CommandIdEntity that = (CommandIdEntity) o;
    return Objects.equals(getCommandId(), that.getCommandId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCommandId());
  }
}
