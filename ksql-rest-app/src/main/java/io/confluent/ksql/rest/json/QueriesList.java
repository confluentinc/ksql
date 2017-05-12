package io.confluent.ksql.rest.json;


import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QueriesList extends KSQLStatementResponse {
  private final List<RunningQuery> queries;

  public QueriesList(String statementText, List<RunningQuery> queries) {
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

    public RunningQuery(String queryString, String kafkaTopic, long id) {
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
    if (!(o instanceof QueriesList)) {
      return false;
    }
    QueriesList that = (QueriesList) o;
    return Objects.equals(getQueries(), that.getQueries());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getQueries());
  }
}
