/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.json;

import java.util.Objects;

public class CommandStatus {
  public enum Status { QUEUED, PARSING, EXECUTING, RUNNING, TERMINATED, SUCCESS, ERROR }

  private final Status status;
  private final String message;

  public CommandStatus(Status status, String message) {
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
