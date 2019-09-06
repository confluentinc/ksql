/*
 * Copyright 2019 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.util.ErrorMessageUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME)
@JsonSubTypes({
    @JsonSubTypes.Type(value = KsqlErrorMessage.class, name = "generic_error"),
    @JsonSubTypes.Type(value = KsqlStatementErrorMessage.class, name = "statement_error")
})
public class KsqlErrorMessage {

  private final int errorCode;
  private final String errorMessage;
  private final ImmutableList<String> stackTrace;

  @JsonCreator
  public KsqlErrorMessage(
      @JsonProperty("error_code") final int errorCode,
      @JsonProperty("message") final String message,
      @JsonProperty("stackTrace") final List<String> stackTrace
  ) {
    this.errorCode = errorCode;
    this.errorMessage = message == null ? "" : message;
    this.stackTrace = stackTrace == null ? ImmutableList.of() : ImmutableList.copyOf(stackTrace);
  }

  public KsqlErrorMessage(final int errorCode, final Throwable exception) {
    this(errorCode, ErrorMessageUtil.buildErrorMessage(exception), getStackTraceStrings(exception));
  }

  public KsqlErrorMessage(final int errorCode, final String message) {
    this(errorCode, message, Collections.emptyList());
  }

  static List<String> getStackTraceStrings(final Throwable exception) {
    return Arrays.stream(exception.getStackTrace())
        .map(StackTraceElement::toString)
        .collect(Collectors.toList());
  }

  public int getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return errorMessage;
  }

  public List<String> getStackTrace() {
    return new ArrayList<>(stackTrace);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder();
    sb.append(getMessage());
    sb.append("\n");
    for (final String line : stackTrace) {
      sb.append(line);
      sb.append("\n");
    }
    return sb.toString();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final KsqlErrorMessage that = (KsqlErrorMessage) o;
    return errorCode == that.errorCode
        && Objects.equals(errorMessage, that.errorMessage)
        && Objects.equals(stackTrace, that.stackTrace);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorCode, errorMessage, stackTrace);
  }
}
