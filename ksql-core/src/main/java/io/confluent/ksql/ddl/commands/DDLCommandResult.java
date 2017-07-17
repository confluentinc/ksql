/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.ddl.commands;

public class DDLCommandResult {

  private final boolean isSucceed;
  private final String message;

  public DDLCommandResult(boolean isSucceed) {
    this(isSucceed, "");
  }

  public DDLCommandResult(boolean isSucceed, String message) {
    this.isSucceed = isSucceed;
    this.message = message;
  }

  public boolean isSucceed() {
    return isSucceed;
  }

  public String getMessage() {
    return message;
  }
}
