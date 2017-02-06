/**
 * Copyright 2017 Confluent Inc.
 *
 **/
package io.confluent.kql.planner;

public class PlanException extends RuntimeException {

  public PlanException(final String message) {
    super(message);
  }

}
