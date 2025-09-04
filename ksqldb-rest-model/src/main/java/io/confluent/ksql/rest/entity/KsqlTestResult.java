/*
 * Copyright 2022 Confluent Inc.
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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KsqlTestResult {
  private final boolean result;
  private final String name;
  private final String message;

  public KsqlTestResult(
      @JsonProperty("result") final boolean result,
      @JsonProperty("name") final String name,
      @JsonProperty("message") final String message
  ) {
    this.result = result;
    this.name = name;
    this.message = message;
  }

  public boolean getResult() {
    return result;
  }

  public String getName() {
    return name;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KsqlTestResult)) {
      return false;
    }
    final KsqlTestResult that = (KsqlTestResult) o;
    return getResult() == that.getResult()
        && Objects.equals(getName(), that.getName())
        && Objects.equals(getMessage(), that.getMessage());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getResult(), getName(), getMessage());
  }

  @Override
  public String toString() {
    return String.format("name: %s%nresult: %s%nmessage: %s", name, result, message);
  }
}
