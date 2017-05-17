package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KSQLTopic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeName("topics")
public class TopicsList extends KSQLEntity {
  private final Collection<TopicInfo> topics;

  @JsonCreator
  public TopicsList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("topics")        Collection<TopicInfo> topics
  ) {
    super(statementText);
    this.topics = topics;
  }

  public static TopicsList fromKsqlTopics(String statementText, Collection<KSQLTopic> ksqlTopics) {
    Collection<TopicInfo> topicInfos = ksqlTopics.stream().map(TopicInfo::new).collect(Collectors.toList());
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
    private final String topicName;
    private final String kafkaTopic;
    private final DataSource.DataSourceSerDe format;

    @JsonCreator
    public TopicInfo(
        @JsonProperty("topicName")  String topicName,
        @JsonProperty("kafkaTopic") String kafkaTopic,
        @JsonProperty("format")     DataSource.DataSourceSerDe format
    ) {
      this.topicName = topicName;
      this.kafkaTopic = kafkaTopic;
      this.format = format;
    }

    public TopicInfo(KSQLTopic ksqlTopic) {
      this(ksqlTopic.getTopicName(), ksqlTopic.getKafkaTopicName(), ksqlTopic.getKsqlTopicSerDe().getSerDe());
    }

    public String getTopicName() {
      return topicName;
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
      return Objects.equals(getTopicName(), topicInfo.getTopicName()) &&
          Objects.equals(getKafkaTopic(), topicInfo.getKafkaTopic()) &&
          getFormat() == topicInfo.getFormat();
    }

    @Override
    public int hashCode() {
      return Objects.hash(getTopicName(), getKafkaTopic(), getFormat());
    }
  }
}
