/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.kql.rest.server.computation;

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
}
