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

import io.confluent.ksql.rest.util.EntityUtil;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.connect.data.Field;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import io.confluent.ksql.metastore.StructuredDataSource;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.util.KafkaTopicClient;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;

@JsonTypeName("description")
@JsonSubTypes({})
public class SourceDescription {

  private final String name;
  private final List<RunningQuery> readQueries;
  private final List<RunningQuery> writeQueries;
  private final List<FieldInfo> fields;
  private final String type;
  private final String key;
  private final String timestamp;
  private final String statistics;
  private final String errorStats;
  private final boolean extended;
  private final String format;
  private final String topic;
  private final int partitions;
  private final int replication;

  @JsonCreator
  public SourceDescription(
      @JsonProperty("name") String name,
      @JsonProperty("readQueries") List<RunningQuery> readQueries,
      @JsonProperty("writeQueries") List<RunningQuery> writeQueries,
      @JsonProperty("fields") List<FieldInfo> fields,
      @JsonProperty("type") String type,
      @JsonProperty("key") String key,
      @JsonProperty("timestamp") String timestamp,
      @JsonProperty("statistics") String statistics,
      @JsonProperty("errorStats") String errorStats,
      @JsonProperty("extended") boolean extended,
      @JsonProperty("format") String format,
      @JsonProperty("topic") String topic,
      @JsonProperty("partitions") int partitions,
      @JsonProperty("replication") int replication
  ) {
    this.name = name;
    this.readQueries = Collections.unmodifiableList(readQueries);
    this.writeQueries = Collections.unmodifiableList(writeQueries);
    this.fields = Collections.unmodifiableList(fields);
    this.type = type;
    this.key = key;
    this.timestamp = timestamp;
    this.statistics = statistics;
    this.errorStats = errorStats;
    this.extended = extended;
    this.format = format;
    this.topic = topic;
    this.partitions = partitions;
    this.replication = replication;
  }

  public SourceDescription(
      StructuredDataSource dataSource,
      boolean extended,
      String format,
      List<RunningQuery> readQueries,
      List<RunningQuery> writeQueries,
      KafkaTopicClient topicClient
  ) {
    this(
        dataSource.getName(),
        readQueries,
        writeQueries,
        EntityUtil.buildSourceSchemaEntity(dataSource.getSchema()),
        dataSource.getDataSourceType().getKqlType(),
        Optional.ofNullable(dataSource.getKeyField()).map(Field::name).orElse(""),
        Optional.ofNullable(dataSource.getTimestampExtractionPolicy())
            .map(TimestampExtractionPolicy::timestampField).orElse(""),
        (extended ? MetricCollectors.getStatsFor(dataSource.getTopicName(), false) : ""),
        (extended ? MetricCollectors.getStatsFor(dataSource.getTopicName(), true) : ""),
        extended,
        format,
        dataSource.getKsqlTopic().getKafkaTopicName(),
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

  private static int getPartitions(KafkaTopicClient topicClient, String kafkaTopicName) {
    Map<String, TopicDescription> stringTopicDescriptionMap =
        topicClient.describeTopics(Arrays.asList(kafkaTopicName));
    TopicDescription topicDescription = stringTopicDescriptionMap.values().iterator().next();
    return topicDescription.partitions().size();
  }

  public int getPartitions() {
    return partitions;
  }

  private static int getReplication(KafkaTopicClient topicClient, String kafkaTopicName) {
    Map<String, TopicDescription> stringTopicDescriptionMap =
        topicClient.describeTopics(Arrays.asList(kafkaTopicName));
    TopicDescription topicDescription = stringTopicDescriptionMap.values().iterator().next();
    return topicDescription.partitions().iterator().next().replicas().size();
  }

  public int getReplication() {
    return replication;
  }

  public String getName() {
    return name;
  }

  public List<FieldInfo> getFields() {
    return fields;
  }

  public boolean isExtended() {
    return extended;
  }

  public String getType() {
    return type;
  }

  public String getFormat() {
    return format;
  }

  public String getTopic() {
    return topic;
  }

  public String getKey() {
    return key;
  }

  public List<RunningQuery> getWriteQueries() {
    return writeQueries;
  }

  public List<RunningQuery> getReadQueries() {
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

  private boolean equals2(SourceDescription that) {
    if (!Objects.equals(topic, that.topic)) {
      return false;
    }
    if (!Objects.equals(key, that.key)) {
      return false;
    }
    if (!Objects.equals(writeQueries, that.writeQueries)) {
      return false;
    }
    if (!Objects.equals(readQueries, that.readQueries)) {
      return false;
    }
    if (!Objects.equals(timestamp, that.timestamp)) {
      return false;
    }
    if (!Objects.equals(statistics, that.statistics)) {
      return false;
    }
    if (!Objects.equals(errorStats, that.errorStats)) {
      return false;
    }
    return true;
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
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    if (!Objects.equals(fields, that.fields)) {
      return false;
    }
    if (!Objects.equals(extended, that.extended)) {
      return false;
    }
    if (!Objects.equals(type, that.type)) {
      return false;
    }
    if (!Objects.equals(format, that.format)) {
      return false;
    }
    return equals2(that);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, fields, type, key, timestamp);
  }
}
