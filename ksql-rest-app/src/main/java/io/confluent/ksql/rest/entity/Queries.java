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

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.confluent.ksql.query.QueryId;

@JsonTypeName("queries")
@JsonSubTypes({})
public class Queries extends KsqlEntity {
  private final List<RunningQuery> queries;

  @JsonCreator
  public Queries(
      @JsonProperty("statementText")  String statementText,
      @JsonProperty("queries")        List<RunningQuery> queries
  ) {
    super(statementText);
    this.queries = queries;
  }

  public List<RunningQuery> getQueries() {
    return new ArrayList<>(queries);
  }

  public static class RunningQuery {
    private final String queryString;
    private final String kafkaTopic;
    private final QueryId id;

    @JsonCreator
    public RunningQuery(
        @JsonProperty("queryString") String queryString,
        @JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("id") QueryId id
    ) {
      this.queryString = queryString;
      this.kafkaTopic = kafkaTopic;
      this.id = id;
    }

    public String getQueryString() {
      return queryString;
    }

    public String getKafkaTopic() {
      return kafkaTopic;
    }

    public QueryId getId() {
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
      return getId() == that.getId()
          && Objects.equals(getQueryString(), that.getQueryString())
          && Objects.equals(getKafkaTopic(), that.getKafkaTopic());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getQueryString(), getKafkaTopic(), getId());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof Queries)) {
      return false;
    }
    Queries that = (Queries) o;
    return Objects.equals(getQueries(), that.getQueries());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getQueries());
  }
}
