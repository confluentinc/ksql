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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;

import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.connect.data.Field;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.planner.plan.KsqlStructuredDataOutputNode;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.SchemaUtil;

@JsonTypeName("description")
@JsonSubTypes({})
public class SourceDescription extends KsqlEntity {

  private final String name;
  private final List<String> readQueries;
  private final List<String> writeQueries;
  private final List<FieldSchemaInfo> schema;
  private final String type;
  private final String key;
  private final String timestamp;
  private final String statistics;
  private final String errorStats;
  private final boolean extended;
  private final String serdes;
  private final String kafkaTopic;
  private final String topology;
  private final String executionPlan;
  private final int partitions;
  private final int replication;

  @JsonCreator
  public SourceDescription(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("name") String name,
      @JsonProperty("readQueries") List<String> readQueries,
      @JsonProperty("writeQueries") List<String> writeQueries,
      @JsonProperty("schema") List<FieldSchemaInfo> schema,
      @JsonProperty("type") String type,
      @JsonProperty("key") String key,
      @JsonProperty("timestamp") String timestamp,
      @JsonProperty("statistics") String statistics,
      @JsonProperty("errorStats") String errorStats,
      @JsonProperty("extended") boolean extended,
      @JsonProperty("serdes") String serdes,
      @JsonProperty("kafkaTopic") String kafkaTopic,
      @JsonProperty("topology") String topology,
      @JsonProperty("executionPlan") String executionPlan,
      @JsonProperty("parititions") int partitions,
      @JsonProperty("replication") int replication
  ) {
    super(statementText);
    this.name = name;
    this.readQueries = readQueries;
    this.writeQueries = writeQueries;
    this.schema = schema;
    this.type = type;
    this.key = key;
    this.timestamp = timestamp;
    this.statistics = statistics;
    this.errorStats = errorStats;
    this.extended = extended;
    this.serdes = serdes;
    this.kafkaTopic = kafkaTopic;
    this.topology = topology;
    this.executionPlan = executionPlan;
    this.partitions = partitions;
    this.replication = replication;
  }

  public SourceDescription(
      StructuredDataSource dataSource,
      boolean extended,
      String serdes,
      String topology,
      String executionPlan,
      List<String> readQueries,
      List<String> writeQueries,
      KafkaTopicClient topicClient
  ) {
    this(
        "",
        dataSource.getName(),
        readQueries,
        writeQueries,
        dataSource.getSchema().fields().stream().map(
            field -> {
              return new FieldSchemaInfo(field.name(), SchemaUtil
                  .getSchemaFieldName(field));
            }).collect(Collectors.toList()),
        dataSource.getDataSourceType().getKqlType(),
        Optional.ofNullable(dataSource.getKeyField()).map(Field::name).orElse(""),
        Optional.ofNullable(dataSource.getTimestampField()).map(Field::name).orElse(""),
        (extended ? MetricCollectors.getStatsFor(dataSource.getTopicName(), false) : ""),
        (extended ? MetricCollectors.getStatsFor(dataSource.getTopicName(), true) : ""),
        extended,
        serdes,
        dataSource.getKsqlTopic().getKafkaTopicName(),
        topology,
        executionPlan,
        (
            extended && topicClient != null ? getPartitions(
                topicClient,
                dataSource
                    .getKsqlTopic()
                    .getKafkaTopicName()
            ) : 0
        ),
        (
            extended && topicClient != null ? getReplication(
                topicClient,
                dataSource
                    .getKsqlTopic()
                    .getKafkaTopicName()
            ) : 0
        )
    );
  }

  public SourceDescription(
      KsqlStructuredDataOutputNode outputNode,
      String statementString,
      String name,
      String topoplogy,
      String executionPlan,
      KafkaTopicClient topicClient
  ) {
    this(
        statementString,
        name,
        Collections.EMPTY_LIST,
        Collections.EMPTY_LIST,
        Collections.EMPTY_LIST,
        "QUERY",
        Optional.ofNullable(outputNode.getKeyField()).map(Field::name).orElse(""),
        Optional.ofNullable(outputNode.getTimestampField()).map(Field::name).orElse(""),
        MetricCollectors.getStatsFor(outputNode.getKafkaTopicName(), false),
        MetricCollectors.getStatsFor(outputNode.getKafkaTopicName(), true),
        true,
        outputNode.getTopicSerde().getSerDe().name(),
        outputNode.getKafkaTopicName(),
        topoplogy,
        executionPlan,
        getPartitions(topicClient, outputNode.getKafkaTopicName()),
        getReplication(topicClient, outputNode.getKafkaTopicName())
    );
  }

  private static int getPartitions(KafkaTopicClient topicClient, String kafkaTopicName) {
    Map<String, TopicDescription> stringTopicDescriptionMap =
        topicClient.describeTopics(Arrays.asList(kafkaTopicName));
    TopicDescription topicDescription = stringTopicDescriptionMap.values().iterator().next();
    return topicDescription.partitions().size();
  }

  private static int getReplication(KafkaTopicClient topicClient, String kafkaTopicName) {
    Map<String, TopicDescription> stringTopicDescriptionMap =
        topicClient.describeTopics(Arrays.asList(kafkaTopicName));
    TopicDescription topicDescription = stringTopicDescriptionMap.values().iterator().next();
    return topicDescription.partitions().iterator().next().replicas().size();

  }


  public String getName() {
    return name;
  }

  public List<FieldSchemaInfo> getSchema() {
    return schema;
  }

  public boolean isExtended() {
    return extended;
  }

  public String getType() {
    return type;
  }

  public String getSerdes() {
    return serdes;
  }

  public String getKafkaTopic() {
    return kafkaTopic;
  }

  public String getKey() {
    return key;
  }

  public List<String> getWriteQueries() {
    return writeQueries;
  }

  public List<String> getReadQueries() {
    return readQueries;
  }

  public String getTimestamp() {
    return timestamp;
  }

  public String getStatistics() {
    return statistics;
  }

  public String getErrorStats() {
    return errorStats;
  }

  public String getTopology() {
    return topology;
  }

  public String getExecutionPlan() {
    return executionPlan;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceDescription)) {
      return false;
    }
    SourceDescription that = (SourceDescription) o;
    return Objects.equals(getName(), that.getName())
           && Objects.equals(getSchema(), that.getSchema())
           && getType().equals(that.getType())
           && Objects.equals(getKey(), that.getKey())
           && Objects.equals(getTimestamp(), that.getTimestamp());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getName(), getSchema(), getType(), getKey(), getTimestamp());
  }

  public int getPartitions() {
    return partitions;
  }

  public int getReplication() {
    return replication;
  }

  public static class FieldSchemaInfo {

    private final String name;
    private final String type;

    @JsonCreator
    public FieldSchemaInfo(
        @JsonProperty("name") String name,
        @JsonProperty("type") String type
    ) {
      this.name = name;
      this.type = type;
    }

    public String getName() {
      return name;
    }

    public String getType() {
      return type;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof FieldSchemaInfo)) {
        return false;
      }
      FieldSchemaInfo that = (FieldSchemaInfo) o;
      return Objects.equals(getName(), that.getName())
             && Objects.equals(getType(), that.getType());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getType());
    }
  }
}
