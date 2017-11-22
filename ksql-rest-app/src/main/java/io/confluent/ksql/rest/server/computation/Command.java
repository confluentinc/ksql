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

package io.confluent.ksql.rest.server.computation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;

import java.util.Map;
import java.util.HashMap;
import java.util.Objects;

import io.confluent.ksql.query.QueryId;

@JsonSubTypes({})
public class Command {
  private final String statement;
  private final Map<String, Object> streamsProperties;
  private final QueryId queryId;

  @JsonCreator
  public Command(
      @JsonProperty("statement") String statement,
      @JsonProperty("streamsProperties") Map<String, Object> streamsProperties,
      @JsonProperty("queryId") QueryId queryId
  ) {
    this.statement = statement;
    this.streamsProperties = streamsProperties;
    this.queryId = queryId;
  }

  public String getStatement() {
    return statement;
  }

  public Map<String, Object> getStreamsProperties() {
    return new HashMap<>(streamsProperties);
  }

  public QueryId getQueryId() {
    return queryId;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    Command command = (Command) o;
    return Objects.equals(statement, command.statement) &&
        Objects.equals(streamsProperties, command.streamsProperties) &&
        Objects.equals(queryId, command.queryId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, streamsProperties, queryId);
  }

  @Override
  public String toString() {
    return "Command{" +
        "statement='" + statement + '\'' +
        ", streamsProperties=" + streamsProperties +
        ", queryId=" + queryId +
        '}';
  }
}
