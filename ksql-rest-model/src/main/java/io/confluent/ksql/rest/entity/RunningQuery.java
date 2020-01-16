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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.query.QueryId;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RunningQuery {

  private final String queryString;
  private final Set<String> sinks;
  private final QueryId id;
  private final Optional<String> state;

  @JsonCreator
  public RunningQuery(
      @JsonProperty("queryString") final String queryString,
      @JsonProperty("sinks") final Set<String> sinks,
      @JsonProperty("id") final QueryId id,
      @JsonProperty("state") final Optional<String> state
  ) {
    this.queryString = Objects.requireNonNull(queryString, "queryString");
    this.sinks = Objects.requireNonNull(sinks, "sinks");
    this.id = Objects.requireNonNull(id, "id");
    this.state = Objects.requireNonNull(state, "state");
  }

  public String getQueryString() {
    return queryString;
  }

  @JsonIgnore
  public String getQuerySingleLine() {
    return queryString.replaceAll(System.lineSeparator(), "");
  }

  public Set<String> getSinks() {
    return sinks;
  }

  public QueryId getId() {
    return id;
  }

  public Optional<String> getState() {
    return state;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof RunningQuery)) {
      return false;
    }
    final RunningQuery that = (RunningQuery) o;
    return Objects.equals(id, that.id)
        && Objects.equals(queryString, that.queryString)
        && Objects.equals(sinks, that.sinks)
        && Objects.equals(state, that.state);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, queryString, id, state);
  }
}
