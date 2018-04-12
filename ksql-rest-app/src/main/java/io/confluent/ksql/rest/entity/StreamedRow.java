/**
 * Copyright 2017 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.server.resources.Errors;

import java.util.Objects;

@JsonSubTypes({})
public class StreamedRow {
  private final GenericRow row;
  private final KsqlErrorMessage errorMessage;

  @JsonCreator
  public StreamedRow(
      @JsonProperty("row") GenericRow row,
      @JsonProperty("errorMessage") KsqlErrorMessage errorMessage
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
    this(null, new KsqlErrorMessage(Errors.ERROR_CODE_SERVER_ERROR, exception));
  }

  public GenericRow getRow() {
    return row;
  }

  public KsqlErrorMessage getErrorMessage() {
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
    return Objects.equals(getRow(), that.getRow())
        && Objects.equals(getErrorMessage(), that.getErrorMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getRow(), getErrorMessage());
  }
}
