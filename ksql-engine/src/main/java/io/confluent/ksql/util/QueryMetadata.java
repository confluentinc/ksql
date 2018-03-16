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

import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.planner.plan.OutputNode;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

public class QueryMetadata {
  private static final Logger log = LoggerFactory.getLogger(QueryMetadata.class);
  private final String statementString;
  private final KafkaStreams kafkaStreams;
  private final OutputNode outputNode;
  private final String executionPlan;
  private final DataSource.DataSourceType dataSourceType;
  private final String queryApplicationId;
  private final KafkaTopicClient kafkaTopicClient;
  private final Topology topoplogy;

  public QueryMetadata(final String statementString,
                       final KafkaStreams kafkaStreams,
                       final OutputNode outputNode,
                       final String executionPlan,
                       final DataSource.DataSourceType dataSourceType,
                       final String queryApplicationId,
                       final KafkaTopicClient kafkaTopicClient,
                       final Topology topoplogy) {
    this.statementString = statementString;
    this.kafkaStreams = kafkaStreams;
    this.outputNode = outputNode;
    this.executionPlan = executionPlan;
    this.dataSourceType = dataSourceType;
    this.queryApplicationId = queryApplicationId;
    this.kafkaTopicClient = kafkaTopicClient;
    this.topoplogy = topoplogy;
  }

  public String getStatementString() {
    return statementString;
  }

  public KafkaStreams getKafkaStreams() {
    return kafkaStreams;
  }

  public OutputNode getOutputNode() {
    return outputNode;
  }

  public String getExecutionPlan() {
    return executionPlan;
  }

  public DataSource.DataSourceType getDataSourceType() {
    return dataSourceType;
  }

  public String getQueryApplicationId() {
    return queryApplicationId;
  }

  public Topology getTopology() {
    return topoplogy;
  }

  public void close() {

    kafkaStreams.close();
    if (kafkaStreams.state() == KafkaStreams.State.NOT_RUNNING) {
      kafkaStreams.cleanUp();
      kafkaTopicClient.deleteInternalTopics(queryApplicationId);
    } else {
      log.error("Could not clean up the query with application id: {}. Query status is: {}",
                queryApplicationId, kafkaStreams.state());
    }
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof QueryMetadata)) {
      return false;
    }

    QueryMetadata that = (QueryMetadata) o;

    return Objects.equals(this.statementString, that.statementString)
        && Objects.equals(this.kafkaStreams, that.kafkaStreams)
        && Objects.equals(this.outputNode, that.outputNode)
        && Objects.equals(this.queryApplicationId, that.queryApplicationId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(statementString, kafkaStreams, outputNode, queryApplicationId);
  }

  public void start() {
    log.info("Starting query with application id: {}", queryApplicationId);
    kafkaStreams.start();
  }

  public String getTopologyDescription() {
    return topoplogy.describe().toString();
  }
}
