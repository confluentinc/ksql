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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorInfo {
  private final String name;
  private final ImmutableMap<String, String> config;
  private final ImmutableList<ConnectorTaskId> tasks;
  private final ConnectorType type;

  @JsonCreator
  public ConnectorInfo(
      @JsonProperty("name") final String name,
      @JsonProperty("config") final Map<String, String> config,
      @JsonProperty("tasks") final List<ConnectorTaskId> tasks,
      @JsonProperty("type") final ConnectorType type
  ) {
    this.name = name;
    this.config = ImmutableMap.copyOf(config);
    this.tasks = ImmutableList.copyOf(tasks);
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
    } else if (o != null && this.getClass() == o.getClass()) {
      final ConnectorInfo that = (ConnectorInfo)o;
      return Objects.equals(this.name, that.name)
          && Objects.equals(this.config, that.config)
          && Objects.equals(this.tasks, that.tasks)
          && Objects.equals(this.type, that.type);
    } else {
      return false;
    }
  }

  public int hashCode() {
    return Objects.hash(new Object[]{this.name, this.config, this.type});
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConnectorTaskId implements Serializable, Comparable<ConnectorTaskId> {
    private final String connector;
    private final int task;

    @JsonCreator
    public ConnectorTaskId(
        @JsonProperty("connector") final String connector,
        @JsonProperty("task") final int task
    ) {
      this.connector = connector;
      this.task = task;
    }

    @JsonProperty
    public String connector() {
      return this.connector;
    }

    @JsonProperty
    public int task() {
      return this.task;
    }

    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      } else if (o != null && this.getClass() == o.getClass()) {
        final ConnectorTaskId that = (ConnectorTaskId) o;
        return this.task != that.task ? false : Objects.equals(this.connector, that.connector);
      } else {
        return false;
      }
    }

    public int hashCode() {
      int result = this.connector != null ? this.connector.hashCode() : 0;
      result = 31 * result + this.task;
      return result;
    }

    public String toString() {
      return this.connector + '-' + this.task;
    }

    public int compareTo(final ConnectorTaskId o) {
      final int connectorCmp = this.connector.compareTo(o.connector);
      return connectorCmp != 0 ? connectorCmp : Integer.compare(this.task, o.task);
    }
  }
}
