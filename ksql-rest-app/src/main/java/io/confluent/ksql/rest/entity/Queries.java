/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonTypeName("queries")
public class Queries extends KSQLEntity {
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
    private final long id;

    @JsonCreator
    public RunningQuery(
        @JsonProperty("queryString") String queryString,
        @JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("id") long id
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

    public long getId() {
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
      return getId() == that.getId() &&
          Objects.equals(getQueryString(), that.getQueryString()) &&
          Objects.equals(getKafkaTopic(), that.getKafkaTopic());
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
