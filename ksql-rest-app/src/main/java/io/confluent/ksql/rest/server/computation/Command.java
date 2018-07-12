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

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@JsonSubTypes({})
public class Command {
  private final String statement;
  private final Map<String, Object> overwriteProperties;
  private final Map<String, String> originalProperties;

  @JsonCreator
  public Command(@JsonProperty("statement") final String statement,
                 @JsonProperty("streamsProperties") final Map<String, Object> overwriteProperties,
                 @JsonProperty("originalProperties") final Map<String, String> originalProperties) {
    this.statement = statement;
    this.overwriteProperties = Collections.unmodifiableMap(overwriteProperties);
    this.originalProperties = originalProperties == null
        ? Collections.emptyMap() : Collections.unmodifiableMap(originalProperties);
  }

  @JsonProperty("statement")
  public String getStatement() {
    return statement;
  }

  @JsonProperty("streamsProperties")
  public Map<String, Object> getOverwriteProperties() {
    return Collections.unmodifiableMap(overwriteProperties);
  }

  @JsonProperty("originalProperties")
  public Map<String, String> getOriginalProperties() {
    return originalProperties;
  }

  @Override
  public boolean equals(Object o) {
    return
        o instanceof Command
        && Objects.equals(statement, ((Command)o).statement)
        && Objects.equals(overwriteProperties, ((Command)o).overwriteProperties)
        && Objects.equals(originalProperties, ((Command)o).originalProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, overwriteProperties, originalProperties);
  }

  @Override
  public String toString() {
    return "Command{"
        + "statement='" + statement + '\''
        + ", overwriteProperties=" + overwriteProperties
        + '}';
  }
}
