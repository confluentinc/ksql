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
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import io.confluent.ksql.util.CommonUtils;

// TODO: Add a field for status code
@JsonSubTypes({})
public class ErrorMessage {

  private final String message;
  private final List<String> stackTrace;

  @JsonCreator
  public ErrorMessage(
      @JsonProperty("message") String message,
      @JsonProperty("stackTrace") List<String> stackTrace
  ) {
    this.message = message;
    this.stackTrace = stackTrace;
  }

  private static String buildErrorMessage(Throwable exception) {
    String msg =
        exception.getMessage() != null
        ? exception.getMessage()
        : " ServerError:" + exception.toString();
    String causeMsg = CommonUtils.getErrorCauseMessage(exception);
    return causeMsg.isEmpty() ? msg : msg + "\r\n" + causeMsg;
  }

  public ErrorMessage(Throwable exception) {
    this(buildErrorMessage(exception), getStackTraceStrings(exception));
  }

  public static List<String> getStackTraceStrings(Throwable exception) {
    return Arrays.stream(exception.getStackTrace())
        .map(StackTraceElement::toString)
        .collect(Collectors.toList());
  }

  public String getMessage() {
    return message;
  }

  public List<String> getStackTrace() {
    return new ArrayList<>(stackTrace);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(message);
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
    if (!(o instanceof ErrorMessage)) {
      return false;
    }
    ErrorMessage that = (ErrorMessage) o;
    return Objects.equals(getMessage(), that.getMessage())
           && Objects.equals(getStackTrace(), that.getStackTrace());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getStackTrace());
  }
}
