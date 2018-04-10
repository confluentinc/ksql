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

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.ksql.serde.DataSource;
import io.confluent.ksql.planner.plan.OutputNode;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.Topology;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
  private final Map<String, Object> overriddenProperties;

  public QueryMetadata(final String statementString,
                       final KafkaStreams kafkaStreams,
                       final OutputNode outputNode,
                       final String executionPlan,
                       final DataSource.DataSourceType dataSourceType,
                       final String queryApplicationId,
                       final KafkaTopicClient kafkaTopicClient,
                       final Topology topoplogy,
                       final Map<String, Object> overriddenProperties) {
    this.statementString = statementString;
    this.kafkaStreams = kafkaStreams;
    this.outputNode = outputNode;
    this.executionPlan = executionPlan;
    this.dataSourceType = dataSourceType;
    this.queryApplicationId = queryApplicationId;
    this.kafkaTopicClient = kafkaTopicClient;
    this.topoplogy = topoplogy;
    this.overriddenProperties = overriddenProperties;
  }

  public Map<String, Object> getOverriddenProperties() {
    return overriddenProperties;
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

  public Schema getResultSchema() {
    return outputNode.getSchema();
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

  private Set<String> getInternalSubjectNameSet(SchemaRegistryClient schemaRegistryClient) {
    try {
      final String suffix1 = KsqlConstants.STREAMS_CHANGELOG_TOPIC_SUFFIX
                             + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;
      final String suffix2 = KsqlConstants.STREAMS_REPARTITION_TOPIC_SUFFIX
                             + KsqlConstants.SCHEMA_REGISTRY_VALUE_SUFFIX;

      return schemaRegistryClient.getAllSubjects().stream()
          .filter(subjectName -> subjectName.startsWith(getQueryApplicationId()))
          .filter(subjectName -> subjectName.endsWith(suffix1) || subjectName.endsWith(suffix2))
          .collect(Collectors.toSet());
    } catch (Exception e) {
      // Do nothing! Schema registry clean up is best effort!
      log.warn("Could not clean up the schema registry for query: " + queryApplicationId, e);
    }
    return new HashSet<>();
  }


  public void cleanUpInternalTopicAvroSchemas(SchemaRegistryClient schemaRegistryClient) {
    getInternalSubjectNameSet(schemaRegistryClient).forEach(subjectName -> {
      try {
        schemaRegistryClient.deleteSubject(subjectName);
      } catch (Exception e) {
        log.warn("Could not clean up the schema registry for query: " + queryApplicationId
                 + ", topic: " + subjectName, e);
      }
    });
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
