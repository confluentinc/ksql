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

import io.confluent.ksql.metastore.KsqlTopic;
import io.confluent.ksql.query.QueryId;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.planner.plan.OutputNode;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;

import java.util.Objects;

public class PersistentQueryMetadata extends QueryMetadata {

  private final QueryId id;
  private final Schema resultSchema;
  private final KsqlTopic resultTopic;


  public PersistentQueryMetadata(final String statementString,
                                 final KafkaStreams kafkaStreams,
                                 final OutputNode outputNode,
                                 final String executionPlan,
                                 final QueryId id,
                                 final DataSource.DataSourceType dataSourceType,
                                 final String queryApplicationId,
                                 final KafkaTopicClient kafkaTopicClient,
                                 final Schema resultSchema,
                                 final KsqlTopic resultTopic,
                                 final Topology topology) {
    super(statementString, kafkaStreams, outputNode, executionPlan, dataSourceType,
          queryApplicationId, kafkaTopicClient, topology);
    this.id = id;
    this.resultSchema = resultSchema;
    this.resultTopic = resultTopic;

  }

  public QueryId getId() {
    return id;
  }

  public Schema getResultSchema() {
    return resultSchema;
  }

  public KsqlTopic getResultTopic() {
    return resultTopic;
  }

  public String getEntity() {
    return getOutputNode().getId().toString();
  }

  public DataSource.DataSourceSerDe getResultTopicSerde() {
    if (resultTopic.getKsqlTopicSerDe() == null) {
      throw new KsqlException(String.format("Invalid result topic: %s. Serde cannot be null.",
                                            resultTopic.getName()));
    }
    return resultTopic.getKsqlTopicSerDe().getSerDe();
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
