/*
 * Copyright 2021 Confluent Inc.
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
import io.confluent.ksql.util.ErrorMessageUtil;
import java.util.Arrays;
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

  @JsonCreator
  public KsqlErrorMessage(
      @JsonProperty("error_code") final int errorCode,
      @JsonProperty("message") final String message
  ) {
    this.errorCode = errorCode;
    this.errorMessage = message == null ? "" : message;
  }

  public KsqlErrorMessage(final int errorCode, final Throwable exception) {
    this(errorCode, ErrorMessageUtil.buildErrorMessage(exception));
  }

  static List<String> getStackTraceStrings(final Throwable exception) {
    return Arrays.stream(exception.getStackTrace())
        .map(StackTraceElement::toString)
        .collect(Collectors.toList());
  }

  @JsonProperty("error_code")
  public int getErrorCode() {
    return errorCode;
  }

  public String getMessage() {
    return errorMessage;
  }

  @Override
  public String toString() {
    return errorMessage;
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
        && errorMessage.equals(that.errorMessage);
  }

  @Override
  public int hashCode() {
    return Objects.hash(errorCode, errorMessage);
  }
}
