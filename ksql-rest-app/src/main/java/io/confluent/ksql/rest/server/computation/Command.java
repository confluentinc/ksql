/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.HashMap;
import java.util.Objects;

public class Command {
  private final String statement;
  private final Map<String, Object> streamsProperties;

  @JsonCreator
  public Command(
      @JsonProperty("statement") String statement,
      @JsonProperty("streamsProperties") Map<String, Object> streamsProperties
  ) {
    this.statement = statement;
    this.streamsProperties = streamsProperties;
  }

  public String getStatement() {
    return statement;
  }

  public Map<String, Object> getStreamsProperties() {
    return new HashMap<>(streamsProperties);
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
    return Objects.equals(getStatement(), command.getStatement())
        && Objects.equals(getStreamsProperties(), command.getStreamsProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStatement(), getStreamsProperties());
  }
}
