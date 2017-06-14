/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class Command {
  private final String statement;

  @JsonCreator
  public Command(@JsonProperty("statement") String statement) {
    this.statement = statement;
  }

  public String getStatement() {
    return statement;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Command)) {
      return false;
    }
    Command command = (Command) o;
    return Objects.equals(getStatement(), command.getStatement());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStatement());
  }
}
