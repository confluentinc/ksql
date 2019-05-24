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
import io.confluent.ksql.statement.Checksum;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;

@JsonSubTypes({})
public class Command {
  private final String statement;
  private final Map<String, Object> overwriteProperties;
  private final Map<String, String> originalProperties;
  private final Checksum checksum;
  private final boolean preVersion5;

  @JsonCreator
  public Command(
      @JsonProperty("statement") final String statement,
      @JsonProperty("streamsProperties") final Map<String, Object> overwriteProperties,
      @JsonProperty("originalProperties") final Map<String, String> originalProperties,
      @JsonProperty("checksum") final Checksum checksum
  ) {
    this.statement = statement;
    this.overwriteProperties = Collections.unmodifiableMap(overwriteProperties);
    this.preVersion5 = originalProperties == null;
    this.originalProperties =
        originalProperties == null ? Collections.emptyMap() : originalProperties;
    this.checksum = checksum;
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

  boolean isPreVersion5() {
    return this.preVersion5;
  }

  @Nullable
  public Checksum getChecksum() {
    return checksum;
  }

  @Override
  public boolean equals(final Object o) {
    return
        o instanceof Command
        && Objects.equals(statement, ((Command)o).statement)
        && Objects.equals(overwriteProperties, ((Command)o).overwriteProperties)
        && Objects.equals(originalProperties, ((Command)o).originalProperties)
        && Objects.equals(checksum, ((Command) o).checksum);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statement, overwriteProperties, originalProperties, checksum);
  }

  @Override
  public String toString() {
    return "Command{"
        + "statement='" + statement + '\''
        + ", overwriteProperties=" + overwriteProperties
        + ", checksum=" + checksum
        + '}';
  }
}
