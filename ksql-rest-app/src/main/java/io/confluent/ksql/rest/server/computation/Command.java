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
  private final boolean useOffsetAsQueryID;
  private final Map<String, Object> overwriteProperties;
  private final Map<String, String> originalProperties;
  private final boolean preVersion5;

  @JsonCreator
  public Command(@JsonProperty("statement") final String statement,
                 @JsonProperty("useOffsetAsQueryID") final Boolean useOffsetAsQueryID,
                 @JsonProperty("streamsProperties") final Map<String, Object> overwriteProperties,
                 @JsonProperty("originalProperties") final Map<String, String> originalProperties) {
    this.statement = statement;
    this.useOffsetAsQueryID = useOffsetAsQueryID == null ? false : useOffsetAsQueryID;
    this.overwriteProperties = Collections.unmodifiableMap(overwriteProperties);
    this.preVersion5 = originalProperties == null;
    this.originalProperties =
        originalProperties == null ? Collections.emptyMap() : originalProperties;
  }

  @JsonProperty("statement")
  public String getStatement() {
    return statement;
  }

  @JsonProperty("useOffsetAsQueryID")
  public boolean getUseOffsetAsQueryID() {
    return useOffsetAsQueryID;
  }

  @JsonProperty("streamsProperties")
  public Map<String, Object> getOverwriteProperties() {
    return Collections.unmodifiableMap(overwriteProperties);
  }

  @JsonProperty("originalProperties")
  public Map<String, String> getOriginalProperties() {
    return originalProperties;
  }

  boolean isPreVersion5() {
    return this.preVersion5;
  }

  @Override
  public boolean equals(final Object o) {
    return
        o instanceof Command
        && Objects.equals(statement, ((Command)o).statement)
        && Objects.equals(useOffsetAsQueryID, ((Command)o).useOffsetAsQueryID)
        && Objects.equals(overwriteProperties, ((Command)o).overwriteProperties)
        && Objects.equals(originalProperties, ((Command)o).originalProperties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, useOffsetAsQueryID, overwriteProperties, originalProperties);
  }

  @Override
  public String toString() {
    return "Command{"
        + "statement='" + statement + '\''
        + ",useOffsetAsQueryID=" + useOffsetAsQueryID
        + ", overwriteProperties=" + overwriteProperties
        + '}';
  }
}
