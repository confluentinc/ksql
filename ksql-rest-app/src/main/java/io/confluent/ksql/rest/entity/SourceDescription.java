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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.metastore.model.DataSource;
import io.confluent.ksql.metrics.MetricCollectors;
import io.confluent.ksql.rest.util.EntityUtil;
import io.confluent.ksql.services.KafkaTopicClient;
import io.confluent.ksql.util.timestamp.TimestampExtractionPolicy;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
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

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @JsonCreator
  public SourceDescription(
      @JsonProperty("name") final String name,
      @JsonProperty("readQueries") final List<RunningQuery> readQueries,
      @JsonProperty("writeQueries") final List<RunningQuery> writeQueries,
      @JsonProperty("fields") final List<FieldInfo> fields,
      @JsonProperty("type") final String type,
      @JsonProperty("key") final String key,
      @JsonProperty("timestamp") final String timestamp,
      @JsonProperty("statistics") final String statistics,
      @JsonProperty("errorStats") final String errorStats,
      @JsonProperty("extended") final boolean extended,
      @JsonProperty("format") final String format,
      @JsonProperty("topic") final String topic,
      @JsonProperty("partitions") final int partitions,
      @JsonProperty("replication") final int replication
  ) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
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
      final DataSource<?> dataSource,
      final boolean extended,
      final String format,
      final List<RunningQuery> readQueries,
      final List<RunningQuery> writeQueries,
      final KafkaTopicClient topicClient
  ) {
    this(
        dataSource.getName(),
        readQueries,
        writeQueries,
        EntityUtil.buildSourceSchemaEntity(dataSource.getSchema(), false),
        dataSource.getDataSourceType().getKsqlType(),
        dataSource.getKeyField().name().orElse(""),
        Optional.ofNullable(dataSource.getTimestampExtractionPolicy())
            .map(TimestampExtractionPolicy::timestampField).orElse(""),
        (extended
            ? MetricCollectors.getAndFormatStatsFor(
                dataSource.getKafkaTopicName(), false) : ""),
        (extended
            ? MetricCollectors.getAndFormatStatsFor(
                dataSource.getKafkaTopicName(), true) : ""),
        extended,
        format,
        dataSource.getKafkaTopicName(),
        (
            extended && topicClient != null ? getPartitions(
                topicClient,
                dataSource.getKafkaTopicName()
            ) : 0
        ),
        (
            extended && topicClient != null ? getReplication(
                topicClient,
                dataSource.getKafkaTopicName()
            ) : 0
        )
    );
  }

  private static int getPartitions(
      final KafkaTopicClient topicClient,
      final String kafkaTopicName
  ) {
    return topicClient
        .describeTopic(kafkaTopicName)
        .partitions()
        .size();
  }

  public int getPartitions() {
    return partitions;
  }

  private static int getReplication(
      final KafkaTopicClient topicClient,
      final String kafkaTopicName
  ) {
    return topicClient
        .describeTopic(kafkaTopicName)
        .partitions().iterator().next()
        .replicas()
        .size();
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

  private boolean equals2(final SourceDescription that) {
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof SourceDescription)) {
      return false;
    }
    final SourceDescription that = (SourceDescription) o;
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
