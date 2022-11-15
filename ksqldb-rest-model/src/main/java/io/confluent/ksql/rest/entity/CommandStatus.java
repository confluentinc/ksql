/*
 * Copyright 2018 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.query.QueryId;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("commandStatus")
@JsonSubTypes({})
public class CommandStatus {
  public enum Status { QUEUED, PARSING, EXECUTING, RUNNING, TERMINATED, SUCCESS, ERROR }

  private final Status status;
  private final String message;
  private final Optional<QueryId> queryId;

  public CommandStatus(final Status status, final String message) {
    this(status, message, Optional.empty());
  }

  @JsonCreator
  public CommandStatus(
      @JsonProperty("status") final Status status,
      @JsonProperty("message") final String message,
      @JsonProperty("queryId") final Optional<QueryId> queryId) {
    this.status = status;
    this.message = message;
    this.queryId = queryId;
  }

  public Status getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public Optional<QueryId> getQueryId() {
    return queryId;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof CommandStatus)) {
      return false;
    }
    final CommandStatus that = (CommandStatus) o;
    return getStatus() == that.getStatus()
        && Objects.equals(getMessage(), that.getMessage())
        && Objects.equals(getQueryId(), that.getQueryId());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStatus(), getMessage(), getQueryId());
  }

  @Override
  public String toString() {
    return status.name() + ": " + message + ". "
        + "Query ID: " + (queryId.isPresent() ? queryId.toString() : "<empty>");
  }
}
