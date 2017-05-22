/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName("commandStatus")
@JsonTypeInfo(
    include = JsonTypeInfo.As.WRAPPER_OBJECT,
    use = JsonTypeInfo.Id.NAME
)
public class CommandStatus {
  public enum Status { QUEUED, PARSING, EXECUTING, RUNNING, TERMINATED, SUCCESS, ERROR }

  private final Status status;
  private final String message;

  @JsonCreator
  public CommandStatus(
      @JsonProperty("status")  Status status,
      @JsonProperty("message") String message) {
    this.status = status;
    this.message = message;
  }

  public Status getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandStatus)) {
      return false;
    }
    CommandStatus that = (CommandStatus) o;
    return getStatus() == that.getStatus() &&
        Objects.equals(getMessage(), that.getMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStatus(), getMessage());
  }
}
