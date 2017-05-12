package io.confluent.ksql.rest.json;

import io.confluent.ksql.metastore.KSQLTopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class TopicsList extends KSQLStatementResponse {
  private final Collection<KSQLTopic> topics;

  public TopicsList(String statementText, Collection<KSQLTopic> topics) {
    super(statementText);
    this.topics = topics;
  }

  public List<KSQLTopic> getTopics() {
    return new ArrayList<>(topics);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TopicsList)) {
      return false;
    }
    TopicsList that = (TopicsList) o;
    return Objects.equals(getTopics(), that.getTopics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTopics());
  }
}
