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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.confluent.ksql.util.ErrorMessageUtil;
import io.confluent.rest.entities.ErrorMessage;

@JsonTypeInfo(
    use = JsonTypeInfo.Id.NAME,
    include = JsonTypeInfo.As.PROPERTY)
@JsonSubTypes({
    @JsonSubTypes.Type(value = KsqlErrorMessage.class, name = "generic_error"),
    @JsonSubTypes.Type(value = KsqlStatementErrorMessage.class, name = "statement_error")})
public class KsqlErrorMessage extends ErrorMessage {

  private final List<String> stackTrace;

  @JsonCreator
  public KsqlErrorMessage(
      @JsonProperty("error_code") int errorCode,
      @JsonProperty("message") String message,
      @JsonProperty("stackTrace") List<String> stackTrace) {
    super(errorCode, message);
    this.stackTrace = stackTrace;
  }

  public KsqlErrorMessage(int errorCode, Throwable exception) {
    this(errorCode, ErrorMessageUtil.buildErrorMessage(exception), getStackTraceStrings(exception));
  }

  public KsqlErrorMessage(int errorCode, String message) {
    this(errorCode, message, Collections.emptyList());
  }

  protected static List<String> getStackTraceStrings(Throwable exception) {
    return Arrays.stream(exception.getStackTrace())
        .map(StackTraceElement::toString)
        .collect(Collectors.toList());
  }

  public List<String> getStackTrace() {
    return new ArrayList<>(stackTrace);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(getMessage());
    sb.append("\n");
    for (String line : stackTrace) {
      sb.append(line);
      sb.append("\n");
    }
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KsqlErrorMessage)) {
      return false;
    }
    KsqlErrorMessage that = (KsqlErrorMessage) o;
    return Objects.equals(getMessage(), that.getMessage())
           && Objects.equals(getStackTrace(), that.getStackTrace());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getStackTrace());
  }
}
