package io.confluent.ksql.rest.json;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class KSQLError extends KSQLStatementResponse {

  private final Error error;

  public KSQLError(String statementText, Throwable exception) {
    super(statementText);
    this.error = new Error(exception);
  }

  public Error getError() {
    return error;
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
    return Objects.equals(getError(), ksqlError.getError());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getError());
  }

  private static class Error {
    private final String message;
    private final List<String> stackTrace;

    public Error(Throwable exception) {
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
      if (!(o instanceof Error)) {
        return false;
      }
      Error error = (Error) o;
      return Objects.equals(getMessage(), error.getMessage()) &&
          Objects.equals(getStackTrace(), error.getStackTrace());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getMessage(), getStackTrace());
    }
  }
}
