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

package io.confluent.ksql.util;

import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.planner.plan.OutputNode;

import org.apache.kafka.streams.KafkaStreams;

import java.util.Objects;

public class PersistentQueryMetadata extends QueryMetadata {

  private final QueryId id;


  public PersistentQueryMetadata(final String statementString,
                                 final KafkaStreams kafkaStreams,
                                 final OutputNode outputNode,
                                 final String executionPlan,
                                 final QueryId id,
                                 final DataSource.DataSourceType dataSourceType,
                                 final String queryApplicationId,
                                 final KafkaTopicClient kafkaTopicClient,
                                 final KsqlConfig ksqlConfig) {
    super(statementString, kafkaStreams, outputNode, executionPlan, dataSourceType,
          queryApplicationId, kafkaTopicClient, ksqlConfig);
    this.id = id;

  }

  public QueryId getId() {
    return id;
  }

  public String getEntity() {
    return getOutputNode().getId().toString();
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof PersistentQueryMetadata)) {
      return false;
    }

    PersistentQueryMetadata that = (PersistentQueryMetadata) o;

    return Objects.equals(this.id, that.id) && super.equals(o);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, super.hashCode());
  }
}
