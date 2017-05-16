package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.KSQLTopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@JsonTypeName("topics")
public class TopicsList extends KSQLEntity {
  private final Collection<KSQLTopic> topics;

  public TopicsList(String statementText, Collection<KSQLTopic> topics) {
    super(statementText);
    this.topics = topics;
  }

  @JsonUnwrapped
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
