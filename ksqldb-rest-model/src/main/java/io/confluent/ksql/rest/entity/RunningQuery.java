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
import io.confluent.ksql.util.KsqlConstants;
import io.confluent.ksql.util.KsqlConstants.KsqlQueryType;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@JsonIgnoreProperties(ignoreUnknown = true)
public class RunningQuery {

  private final String queryString;
  private final Set<String> sinks;
  private final Set<String> sinkKafkaTopics;
  private final QueryId id;
  private final QueryStatusCount statusCount;
  private final KsqlConstants.KsqlQueryType queryType;

  @JsonCreator
  public RunningQuery(
      @JsonProperty("queryString") final String queryString,
      @JsonProperty("sinks") final Set<String> sinks,
      @JsonProperty("sinkKafkaTopics") final Set<String> sinkKafkaTopics,
      @JsonProperty("id") final QueryId id,
      @JsonProperty("statusCount") final QueryStatusCount statusCount,
      @JsonProperty("queryType") final KsqlQueryType queryType
  ) {
    this.queryString = Objects.requireNonNull(queryString, "queryString");
    this.sinkKafkaTopics = Objects.requireNonNull(sinkKafkaTopics, "sinkKafkaTopics");
    this.sinks = Objects.requireNonNull(sinks, "sinks");
    this.id = Objects.requireNonNull(id, "id");
    this.statusCount = Objects.requireNonNull(statusCount, "statusCount");
    this.queryType = Objects.requireNonNull(queryType, "queryType");
  }

  public String getQueryString() {
    return queryString;
  }

  @JsonIgnore
  public String getQuerySingleLine() {
    return queryString.replaceAll(System.lineSeparator(), " ");
  }

  public Set<String> getSinks() {
    return Collections.unmodifiableSet(sinks);
  }

  public Set<String> getSinkKafkaTopics() {
    return Collections.unmodifiableSet(sinkKafkaTopics);
  }

  public QueryId getId() {
    return id;
  }

  // kept for backwards compatibility
  @JsonProperty("state")
  public Optional<String> getState() {
    return Optional.of(statusCount.getAggregateStatus().toString());
  }

  public QueryStatusCount getStatusCount() {
    return statusCount;
  }

  public KsqlQueryType getQueryType() {
    return queryType;
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
        && Objects.equals(sinkKafkaTopics, that.sinkKafkaTopics)
        && Objects.equals(statusCount, that.statusCount);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, queryString, sinks, sinkKafkaTopics, statusCount);
  }
}
