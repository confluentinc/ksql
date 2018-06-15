/*
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

import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import io.confluent.ksql.GenericRow;
import io.confluent.ksql.rest.server.resources.Errors;

@JsonSubTypes({})
public class StreamedRow {

  private final GenericRow row;
  private final KsqlErrorMessage errorMessage;
  private final String finalMessage;

  public static StreamedRow row(final GenericRow row) {
    return new StreamedRow(row, null, null);
  }

  public static StreamedRow error(final Throwable exception) {
    return new StreamedRow(
        null,
        new KsqlErrorMessage(Errors.ERROR_CODE_SERVER_ERROR, exception),
        null);
  }

  public static StreamedRow finalMessage(final String finalMessage) {
    return new StreamedRow(null, null, finalMessage);
  }

  @JsonCreator
  public StreamedRow(
      @JsonProperty("row") final GenericRow row,
      @JsonProperty("errorMessage") final KsqlErrorMessage errorMessage,
      @JsonProperty("finalMessage") final String finalMessage
  ) {
    checkUnion(row, errorMessage, finalMessage);
    this.row = row;
    this.errorMessage = errorMessage;
    this.finalMessage = finalMessage;
  }

  public GenericRow getRow() {
    return row;
  }

  public KsqlErrorMessage getErrorMessage() {
    return errorMessage;
  }

  public String getFinalMessage() {
    return finalMessage;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final StreamedRow that = (StreamedRow) o;
    return Objects.equals(row, that.row)
           && Objects.equals(errorMessage, that.errorMessage)
           && Objects.equals(finalMessage, that.finalMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(row, errorMessage, finalMessage);
  }

  private static void checkUnion(Object... fields) {
    final List<Object> fs = Arrays.asList(fields);
    final long count = fs.stream()
        .filter(Objects::nonNull)
        .count();

    if (count != 1) {
      throw new IllegalArgumentException("Exactly one parameter should be non-null. got: " + fs);
    }
  }
}
