/*
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
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.serde.DataSource;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class KsqlTopicInfo {
  private final String name;
  private final String kafkaTopic;
  private final DataSource.DataSourceSerDe format;

  @JsonCreator
  public KsqlTopicInfo(
      @JsonProperty("name") final String name,
      @JsonProperty("kafkaTopic") final String kafkaTopic,
      @JsonProperty("format") final DataSource.DataSourceSerDe format
  ) {
    this.name = name;
    this.kafkaTopic = kafkaTopic;
    this.format = format;
  }

  public KsqlTopicInfo(final KsqlTopic ksqlTopic) {
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KsqlTopicInfo)) {
      return false;
    }
    final KsqlTopicInfo topicInfo = (KsqlTopicInfo) o;
    return Objects.equals(getName(), topicInfo.getName())
        && Objects.equals(getKafkaTopic(), topicInfo.getKafkaTopic())
        && getFormat() == topicInfo.getFormat();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getKafkaTopic(), getFormat());
  }
}