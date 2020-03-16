/*
 * Copyright 2019 Confluent Inc.
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
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class KafkaTopicsListExtended extends KsqlEntity {

  private final Collection<KafkaTopicInfoExtended> topics;

  @JsonCreator
  public KafkaTopicsListExtended(
      @JsonProperty("statementText") final String statementText,
      @JsonProperty("topics") final Collection<KafkaTopicInfoExtended> topics
  ) {
    super(statementText);
    Preconditions.checkNotNull(topics, "topics field must not be null");
    this.topics = topics;
  }

  public List<KafkaTopicInfoExtended> getTopics() {
    return new ArrayList<>(topics);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KafkaTopicsListExtended)) {
      return false;
    }
    final KafkaTopicsListExtended that = (KafkaTopicsListExtended) o;
    return Objects.equals(getTopics(), that.getTopics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTopics());
  }
}
