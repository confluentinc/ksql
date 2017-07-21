/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.KsqlTopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@JsonTypeName("topics")
public class KsqlTopicsList extends KsqlEntity {
  private final Collection<KsqlTopicInfo> topics;

  @JsonCreator
  public KsqlTopicsList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("topics")        Collection<KsqlTopicInfo> topics
  ) {
    super(statementText);
    this.topics = topics;
  }

  @JsonUnwrapped
  public List<KsqlTopicInfo> getTopics() {
    return new ArrayList<>(topics);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KsqlTopicsList)) {
      return false;
    }
    KsqlTopicsList that = (KsqlTopicsList) o;
    return Objects.equals(getTopics(), that.getTopics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTopics());
  }

  public static KsqlTopicsList buildFromKsqlTopics(String statementText, Collection<KsqlTopic> ksqlTopics) {
    List<KsqlTopicInfo> ksqlTopicInfoList = new ArrayList<>();
    for (KsqlTopic ksqlTopic: ksqlTopics) {
      ksqlTopicInfoList.add(new KsqlTopicInfo(ksqlTopic));
    }
    return new KsqlTopicsList(statementText, ksqlTopicInfoList);
  }

}
