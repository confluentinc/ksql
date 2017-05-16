package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonTypeName("error")
public class KSQLError extends KSQLEntity {

  private final String message;
  private final List<String> stackTrace;

  public KSQLError(String statementText, Throwable exception) {
    super(statementText);
    this.message = exception.getMessage();

    this.stackTrace = new ArrayList<>(exception.getStackTrace().length);
    for (StackTraceElement stackTraceElement : exception.getStackTrace()) {
      this.stackTrace.add(stackTraceElement.toString());
    }
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
    if (!(o instanceof KSQLError)) {
      return false;
    }
    KSQLError ksqlError = (KSQLError) o;
    return Objects.equals(getMessage(), ksqlError.getMessage()) &&
        Objects.equals(getStackTrace(), ksqlError.getStackTrace());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getStackTrace());
  }
}
