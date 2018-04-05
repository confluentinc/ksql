/**
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

import com.google.common.base.Preconditions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import io.confluent.ksql.metastore.KsqlTopic;

@JsonTypeName("ksql_topics")
@JsonSubTypes({})
public class KsqlTopicsList extends KsqlEntity {
  private final Collection<KsqlTopicInfo> topics;

  @JsonCreator
  public KsqlTopicsList(
          @JsonProperty("statementText") String statementText,
          @JsonProperty("topics")   Collection<KsqlTopicInfo> topics
  ) {
    super(statementText);
    Preconditions.checkNotNull(topics, "topics field must not be null");
    this.topics = topics;
  }

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

  public static KsqlTopicsList build(String statementText, Collection<KsqlTopic> ksqlTopics) {
    List<KsqlTopicInfo> ksqlTopicInfoList = new ArrayList<>();
    for (KsqlTopic ksqlTopic: ksqlTopics) {
      ksqlTopicInfoList.add(new KsqlTopicInfo(ksqlTopic));
    }
    return new KsqlTopicsList(statementText, ksqlTopicInfoList);
  }

}
