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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.confluent.ksql.metastore.KsqlTopic;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

public class KsqlTopicsList extends KsqlEntity {
  private final Collection<KsqlTopicInfo> topics;

  @JsonCreator
  public KsqlTopicsList(
          @JsonProperty("statementText") final String statementText,
          @JsonProperty("topics") final Collection<KsqlTopicInfo> topics
  ) {
    super(statementText);
    Preconditions.checkNotNull(topics, "topics field must not be null");
    this.topics = topics;
  }

  public List<KsqlTopicInfo> getTopics() {
    return new ArrayList<>(topics);
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof KsqlTopicsList)) {
      return false;
    }
    final KsqlTopicsList that = (KsqlTopicsList) o;
    return Objects.equals(getTopics(), that.getTopics());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getTopics());
  }

  public static KsqlTopicsList build(
      final String statementText,
      final Collection<KsqlTopic> ksqlTopics) {
    final List<KsqlTopicInfo> ksqlTopicInfoList = new ArrayList<>();
    for (final KsqlTopic ksqlTopic: ksqlTopics) {
      ksqlTopicInfoList.add(new KsqlTopicInfo(ksqlTopic));
    }
    return new KsqlTopicsList(statementText, ksqlTopicInfoList);
  }

}
