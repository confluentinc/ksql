/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.physical.GenericRow;

import java.util.Objects;

public class StreamedRow {
  private final GenericRow row;
  private final ErrorMessage errorMessage;

  @JsonCreator
  public StreamedRow(
      @JsonProperty("row") GenericRow row,
      @JsonProperty("errorMessage") ErrorMessage errorMessage
  ) {
    if ((row == null) == (errorMessage == null)) {
      throw new IllegalArgumentException("Exactly one of row and error message must be null");
    }
    this.row = row;
    this.errorMessage = errorMessage;
  }

  public StreamedRow(GenericRow row) {
    this(row, null);
  }

  public StreamedRow(Throwable exception) {
    this(null, new ErrorMessage(exception));
  }

  public GenericRow getRow() {
    return row;
  }

  public ErrorMessage getErrorMessage() {
    return errorMessage;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamedRow)) {
      return false;
    }
    StreamedRow that = (StreamedRow) o;
    return Objects.equals(getRow(), that.getRow()) &&
        Objects.equals(getErrorMessage(), that.getErrorMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRow(), getErrorMessage());
  }
}
