/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.json;

import io.confluent.ksql.rest.server.computation.CommandId;

import java.util.Objects;

public class CommandIdResponse extends KSQLStatementResponse {

  private final CommandId commandId;

  public CommandIdResponse(String statementText, CommandId commandId) {
    super(statementText);
    this.commandId = commandId;
  }

  public CommandId getCommandId() {
    return commandId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandIdResponse)) {
      return false;
    }
    CommandIdResponse that = (CommandIdResponse) o;
    return Objects.equals(getCommandId(), that.getCommandId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getCommandId());
  }
}
