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

package io.confluent.support.metrics.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class ErrorResponse {

  private final String message;

  @JsonCreator
  public ErrorResponse(@JsonProperty("message") final String message) {
    this.message = message;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof io.confluent.support.metrics.response.ErrorResponse)) {
      return false;
    }
    final io.confluent.support.metrics.response.ErrorResponse that =
        (io.confluent.support.metrics.response.ErrorResponse) o;
    return Objects.equals(getMessage(), that.getMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getMessage(), getMessage());
  }

}
