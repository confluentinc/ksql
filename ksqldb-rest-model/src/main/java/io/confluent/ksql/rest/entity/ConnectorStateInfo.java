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
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ConnectorStateInfo {
  public static final String RUNNING = "RUNNING";

  private final String name;
  private final ConnectorState connector;
  private final ImmutableList<TaskState> tasks;
  private final ConnectorType type;

  @JsonCreator
  public ConnectorStateInfo(
      @JsonProperty("name") final String name,
      @JsonProperty("connector") final ConnectorState connector,
      @JsonProperty("tasks") final List<TaskState> tasks,
      @JsonProperty("type") final ConnectorType type
  ) {
    this.name = name;
    this.connector = connector;
    this.tasks = ImmutableList.copyOf(tasks);
    this.type = type;
  }

  @JsonProperty
  public String name() {
    return this.name;
  }

  @JsonProperty
  public ConnectorState connector() {
    return this.connector;
  }

  @JsonProperty
  public List<TaskState> tasks() {
    return this.tasks;
  }

  @JsonProperty
  public ConnectorType type() {
    return this.type;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class TaskState extends AbstractState implements Comparable<TaskState> {
    private final int id;

    @JsonCreator
    public TaskState(
        @JsonProperty("id") final int id,
        @JsonProperty("state") final String state,
        @JsonProperty("worker_id") final String worker,
        @JsonProperty("msg") final String msg
    ) {
      super(state, worker, msg);
      this.id = id;
    }

    @JsonProperty
    public int id() {
      return this.id;
    }

    public int compareTo(final TaskState that) {
      return Integer.compare(this.id, that.id);
    }

    public boolean equals(final Object o) {
      if (o == this) {
        return true;
      } else if (!(o instanceof TaskState)) {
        return false;
      } else {
        final TaskState other = (TaskState) o;
        return this.compareTo(other) == 0;
      }
    }

    public int hashCode() {
      return Objects.hash(new Object[]{this.id});
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class ConnectorState extends AbstractState {
    @JsonCreator
    public ConnectorState(
        @JsonProperty("state") final String state,
        @JsonProperty("worker_id") final String worker,
        @JsonProperty("msg") final String msg
    ) {
      super(state, worker, msg);
    }
  }

  public abstract static class AbstractState {
    private final String state;
    private final String trace;
    private final String workerId;

    public AbstractState(final String state, final String workerId, final String trace) {
      this.state = state;
      this.workerId = workerId;
      this.trace = trace;
    }

    @JsonProperty
    public String state() {
      return this.state;
    }

    @JsonProperty("worker_id")
    public String workerId() {
      return this.workerId;
    }

    @JsonProperty
    @JsonInclude(Include.NON_EMPTY)
    public String trace() {
      return this.trace;
    }
  }
}
