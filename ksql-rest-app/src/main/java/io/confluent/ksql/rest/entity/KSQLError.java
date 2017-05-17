package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeName("error")
public class KSQLError extends KSQLEntity {

  private final String message;
  private final List<String> stackTrace;

  @JsonCreator
  public KSQLError(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("message")       String message,
      @JsonProperty("stackTrace")    List<String> stackTrace
  ) {
    super(statementText);
    this.message = message;
    this.stackTrace = stackTrace;
  }

  public KSQLError(String statementText, Throwable exception) {
    this(statementText, exception.getMessage(), getStackTraceStrings(exception));
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
