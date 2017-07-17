/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

public class DDLCommandResult {

  private final boolean success;
  private final String message;

  public DDLCommandResult(boolean success) {
    this(success, "");
  }

  public DDLCommandResult(boolean success, String message) {
    this.success = success;
    this.message = message;
  }

  public boolean isSuccess() {
    return success;
  }

  public String getMessage() {
    return message;
  }
}
