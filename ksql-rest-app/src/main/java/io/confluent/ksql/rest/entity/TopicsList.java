/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KsqlTopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeName("topics")
public class TopicsList extends KsqlEntity {
  private final Collection<TopicInfo> topics;

  @JsonCreator
  public TopicsList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("topics")        Collection<TopicInfo> topics
  ) {
    super(statementText);
    this.topics = topics;
  }

  public static TopicsList fromKsqlTopics(String statementText, Collection<KsqlTopic> ksqlTopics) {
    Collection<TopicInfo> topicInfos =
        ksqlTopics.stream().map(TopicInfo::new).collect(Collectors.toList());
    return new TopicsList(statementText, topicInfos);
  }

  @JsonUnwrapped
  public List<TopicInfo> getTopics() {
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

  public static class TopicInfo {
    private final String name;
    private final String kafkaTopic;
    private final DataSource.DataSourceSerDe format;

    @JsonCreator
    public TopicInfo(
        @JsonProperty("name")       String name,
        @JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("format")     DataSource.DataSourceSerDe format
    ) {
      this.name = name;
      this.kafkaTopic = kafkaTopic;
      this.format = format;
    }

    public TopicInfo(KsqlTopic ksqlTopic) {
      this(
          ksqlTopic.getTopicName(),
          ksqlTopic.getKafkaTopicName(),
          ksqlTopic.getKsqlTopicSerDe().getSerDe()
      );
    }

    public String getName() {
      return name;
    }

    public String getKafkaTopic() {
      return kafkaTopic;
    }

    public DataSource.DataSourceSerDe getFormat() {
      return format;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof TopicInfo)) {
        return false;
      }
      TopicInfo topicInfo = (TopicInfo) o;
      return Objects.equals(getName(), topicInfo.getName())
          && Objects.equals(getKafkaTopic(), topicInfo.getKafkaTopic())
          && getFormat() == topicInfo.getFormat();
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getKafkaTopic(), getFormat());
    }
  }
}
