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
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import io.confluent.ksql.model.WindowType;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeName("description")
@JsonSubTypes({})
public class SourceDescription {

  private final String name;
  private final Optional<WindowType> windowType;
  private final List<RunningQuery> readQueries;
  private final List<RunningQuery> writeQueries;
  private final List<FieldInfo> fields;
  private final String type;
  private final String timestamp;
  private final String statistics;
  private final String errorStats;
  private final boolean extended;
  private final String keyFormat;
  private final String valueFormat;
  private final String topic;
  private final int partitions;
  private final int replication;
  private final String statement;
  private final List<QueryOffsetSummary> queryOffsetSummaries;

  // CHECKSTYLE_RULES.OFF: ParameterNumberCheck
  @JsonCreator
  public SourceDescription(
      @JsonProperty("name") final String name,
      @JsonProperty("windowType") final Optional<WindowType> windowType,
      @JsonProperty("readQueries") final List<RunningQuery> readQueries,
      @JsonProperty("writeQueries") final List<RunningQuery> writeQueries,
      @JsonProperty("fields") final List<FieldInfo> fields,
      @JsonProperty("type") final String type,
      @JsonProperty("timestamp") final String timestamp,
      @JsonProperty("statistics") final String statistics,
      @JsonProperty("errorStats") final String errorStats,
      @JsonProperty("extended") final boolean extended,
      @JsonProperty("keyFormat") final String keyFormat,
      @JsonProperty("valueFormat") final String valueFormat,
      @JsonProperty("topic") final String topic,
      @JsonProperty("partitions") final int partitions,
      @JsonProperty("replication") final int replication,
      @JsonProperty("statement") final String statement,
      @JsonProperty("queryOffsetSummaries") final List<QueryOffsetSummary> queryOffsetSummaries) {
    // CHECKSTYLE_RULES.ON: ParameterNumberCheck
    this.name = Objects.requireNonNull(name, "name");
    this.windowType = Objects.requireNonNull(windowType, "windowType");
    this.readQueries =
        ImmutableList.copyOf(Objects.requireNonNull(readQueries, "readQueries"));
    this.writeQueries =
        ImmutableList.copyOf(Objects.requireNonNull(writeQueries, "writeQueries"));
    this.fields =
        ImmutableList.copyOf(Objects.requireNonNull(fields, "fields"));
    this.type = Objects.requireNonNull(type, "type");
    this.timestamp = Objects.requireNonNull(timestamp, "timestamp");
    this.statistics = Objects.requireNonNull(statistics, "statistics");
    this.errorStats = Objects.requireNonNull(errorStats, "errorStats");
    this.extended = extended;
    this.keyFormat = Objects.requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = Objects.requireNonNull(valueFormat, "valueFormat");
    this.topic = Objects.requireNonNull(topic, "topic");
    this.partitions = partitions;
    this.replication = replication;
    this.statement = Objects.requireNonNull(statement, "statement");
    this.queryOffsetSummaries = ImmutableList.copyOf(
        Objects.requireNonNull(queryOffsetSummaries, "queryOffsetSummaries"));
  }

  public String getStatement() {
    return statement;
  }

  public Optional<WindowType> getWindowType() {
    return windowType;
  }

  public int getPartitions() {
    return partitions;
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

  public String getKeyFormat() {
    return keyFormat;
  }

  public String getValueFormat() {
    return valueFormat;
  }

  public String getTopic() {
    return topic;
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

  public List<QueryOffsetSummary> getQueryOffsetSummaries() {
    return queryOffsetSummaries;
  }

  // CHECKSTYLE_RULES.OFF: CyclomaticComplexity
  @Override
  public boolean equals(final Object o) {
    // CHECKSTYLE_RULES.ON: CyclomaticComplexity
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SourceDescription that = (SourceDescription) o;
    return extended == that.extended
        && partitions == that.partitions
        && replication == that.replication
        && Objects.equals(name, that.name)
        && Objects.equals(windowType, that.windowType)
        && Objects.equals(readQueries, that.readQueries)
        && Objects.equals(writeQueries, that.writeQueries)
        && Objects.equals(fields, that.fields)
        && Objects.equals(type, that.type)
        && Objects.equals(timestamp, that.timestamp)
        && Objects.equals(statistics, that.statistics)
        && Objects.equals(errorStats, that.errorStats)
        && Objects.equals(keyFormat, that.keyFormat)
        && Objects.equals(valueFormat, that.valueFormat)
        && Objects.equals(topic, that.topic)
        && Objects.equals(statement, that.statement)
        && Objects.equals(queryOffsetSummaries, that.queryOffsetSummaries);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        name,
        windowType,
        readQueries,
        writeQueries,
        fields,
        type,
        timestamp,
        statistics,
        errorStats,
        extended,
        keyFormat,
        valueFormat,
        topic,
        partitions,
        replication,
        statement,
        queryOffsetSummaries
    );
  }
}
