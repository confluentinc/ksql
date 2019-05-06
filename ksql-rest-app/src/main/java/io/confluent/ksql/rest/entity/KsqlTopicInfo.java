/*
 * Copyright 2018 Confluent Inc.
 *
 * Licensed under the Confluent Community License (the "License"); you may not use
 * this file except in compliance with the License.  You may obtain a copy of the
 * License at
 *
 * http://www.confluent.io/confluent-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OF ANY KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations under the License.
 */

package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import io.confluent.ksql.metastore.model.KsqlTopic;
import io.confluent.ksql.serde.Format;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonSubTypes({})
public class KsqlTopicInfo {
  private final String name;
  private final String kafkaTopic;
  private final Format format;

  @JsonCreator
  public KsqlTopicInfo(
      @JsonProperty("name") final String name,
      @JsonProperty("kafkaTopic") final String kafkaTopic,
      @JsonProperty("format") final Format format
  ) {
    this.name = name;
    this.kafkaTopic = kafkaTopic;
    this.format = format;
  }

  public KsqlTopicInfo(final KsqlTopic ksqlTopic) {
    this(
        ksqlTopic.getKsqlTopicName(),
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

  public Format getFormat() {
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