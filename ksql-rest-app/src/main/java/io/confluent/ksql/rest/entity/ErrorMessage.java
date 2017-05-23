/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

// TODO: Add a field for status code
public class ErrorMessage {

  private final String message;
  private final List<String> stackTrace;

  @JsonCreator
  public ErrorMessage(
      @JsonProperty("message")       String message,
      @JsonProperty("stackTrace")    List<String> stackTrace
  ) {
    this.message = message;
    this.stackTrace = stackTrace;
  }

  public ErrorMessage(Throwable exception) {
    this(exception.getMessage(), getStackTraceStrings(exception));
  }

  public static List<String> getStackTraceStrings(Throwable exception) {
    return Arrays.stream(exception.getStackTrace()).map(StackTraceElement::toString).collect(Collectors.toList());
  }

  public String getMessage() {
    return message;
  }

  public List<String> getStackTrace() {
    return new ArrayList<>(stackTrace);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ErrorMessage)) {
      return false;
    }
    ErrorMessage that = (ErrorMessage) o;
    return Objects.equals(getMessage(), that.getMessage()) &&
        Objects.equals(getStackTrace(), that.getStackTrace());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getStackTrace());
  }
}