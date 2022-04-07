/*
 * Copyright 2022 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"; you may not use
 * this file except in compliance with the License. You may obtain a copy of the
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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.errorprone.annotations.Immutable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.kafka.connect.util.ConnectorTaskId;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(Include.NON_NULL)
@Immutable
public class ConnectorInfo {
  private final String name;
  private final ImmutableMap<String, String> config;
  private final ImmutableList<ConnectorTaskId> tasks;
  private final ConnectorType type;

  @JsonCreator
  public ConnectorInfo(
      @JsonProperty("name") final String name,
      @JsonProperty("config") final ImmutableMap<String, String> config,
      @JsonProperty("tasks") final ImmutableList<ConnectorTaskId> tasks,
      @JsonProperty("type") final ConnectorType type
  ) {
    this.name = name;
    this.config = config;
    this.tasks = tasks;
    this.type = type;
  }

  @JsonProperty
  public String name() {
    return this.name;
  }

  @JsonProperty
  public ConnectorType type() {
    return this.type;
  }

  @JsonProperty
  public Map<String, String> config() {
    return this.config;
  }

  @JsonProperty
  public List<ConnectorTaskId> tasks() {
    return this.tasks;
  }

  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ConnectorInfo that = (ConnectorInfo) o;
    return Objects.equals(name, that.name)
        && type == that.type
        && Objects.equals(tasks, that.tasks)
        && Objects.equals(config, that.config);
  }

  public int hashCode() {
    return Objects.hash(name, config, tasks, type);
  }
}
