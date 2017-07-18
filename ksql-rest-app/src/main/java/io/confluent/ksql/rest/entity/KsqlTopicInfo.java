/**
 * Copyright 2017 Confluent Inc.
 **/

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.confluent.ksql.metastore.DataSource;
import io.confluent.ksql.metastore.KsqlTopic;

import java.util.Objects;

public class KsqlTopicInfo {
  private final String name;
  private final String kafkaTopic;
  private final DataSource.DataSourceSerDe format;

  @JsonCreator
  public KsqlTopicInfo(
      @JsonProperty("name")       String name,
      @JsonProperty("kafkaTopic") String kafkaTopic,
      @JsonProperty("format")     DataSource.DataSourceSerDe format
  ) {
    this.name = name;
    this.kafkaTopic = kafkaTopic;
    this.format = format;
  }

  public KsqlTopicInfo(KsqlTopic ksqlTopic) {
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
    if (!(o instanceof KsqlTopicInfo)) {
      return false;
    }
    KsqlTopicInfo topicInfo = (KsqlTopicInfo) o;
    return Objects.equals(getName(), topicInfo.getName())
        && Objects.equals(getKafkaTopic(), topicInfo.getKafkaTopic())
        && getFormat() == topicInfo.getFormat();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getKafkaTopic(), getFormat());
  }
}