/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;

import java.util.Objects;

@JsonTypeName("error")
public class ErrorMessageEntity extends KsqlEntity {

  private final ErrorMessage errorMessage;

  @JsonCreator
  public ErrorMessageEntity(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("errorMessage")  ErrorMessage errorMessage
  ) {
    super(statementText);
    this.errorMessage = errorMessage;
  }

  public ErrorMessageEntity(String statementText, Throwable exception) {
    this(statementText, new ErrorMessage(exception));
  }

  @JsonUnwrapped
  public ErrorMessage getErrorMessage() {
    return errorMessage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ErrorMessageEntity)) {
      return false;
    }
    ErrorMessageEntity that = (ErrorMessageEntity) o;
    return Objects.equals(getErrorMessage(), that.getErrorMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getErrorMessage());
  }
}
