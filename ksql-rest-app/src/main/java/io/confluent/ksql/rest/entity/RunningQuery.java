/**
 * Copyright 2018 Confluent Inc.
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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.Set;

public class RunningQuery {
  private final String queryString;
  private final Set<String> sinks;
  private final EntityQueryId id;

  @JsonCreator
  public RunningQuery(
      @JsonProperty("statementText") String queryString,
      @JsonProperty("sinks") Set<String> sinks,
      @JsonProperty("id") EntityQueryId id
  ) {
    this.queryString = queryString;
    this.sinks = sinks;
    this.id = id;
  }

  public String getQueryString() {
    return queryString;
  }

  public Set<String> getSinks() {
    return sinks;
  }

  public EntityQueryId getId() {
    return id;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RunningQuery)) {
      return false;
    }
    RunningQuery that = (RunningQuery) o;
    return Objects.equals(id, that.id)
        && Objects.equals(queryString, that.queryString)
        && Objects.equals(sinks, that.sinks);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, queryString, id);
  }
}
