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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

@JsonSubTypes({})
public class Command {
  private String statement;
  private Map<String, Object> ksqlProperties;

  public Command(String statement,
                 Map<String, Object> ksqlProperties
  ) {
    this.statement = statement;
    this.ksqlProperties = ksqlProperties;
  }

  public Command() {
    this.statement = "";
    this.ksqlProperties = null;
  }

  @JsonProperty("statement")
  public String getStatement() {
    return statement;
  }

  public void setStatement(String statement) {
    this.statement = statement;
  }

  @JsonProperty("streamsProperties")
  public Map<String, Object> getKsqlProperties() {
    return new HashMap<>(ksqlProperties);
  }

  public void setKsqlProperties(Map<String, Object> ksqlProperties) {
    this.ksqlProperties = ksqlProperties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Command)) {
      return false;
    }
    Command command = (Command) o;
    return Objects.equals(getStatement(), command.getStatement())
        && Objects.equals(getKsqlProperties(), command.getKsqlProperties());
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, ksqlProperties);
  }

  @Override
  public String toString() {
    return "Command{"
        + "statement='" + statement + '\''
        + ", ksqlProperties=" + ksqlProperties
        + '}';
  }
}
