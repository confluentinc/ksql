/**
 * Copyright 2018 Confluent Inc.
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
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Objects;

import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.rest.entity.SourceInfo.Stream;
import io.confluent.ksql.rest.entity.SourceInfo.Table;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "STREAM", value = Stream.class),
    @JsonSubTypes.Type(name = "TABLE", value = Table.class),
    })
public class SourceInfo {
  private final String name;
  private final String topic;
  private final String format;

  public SourceInfo(String name, String topic, String format) {
    this.name = name;
    this.topic = topic;
    this.format = format;
  }

  public static class Stream extends SourceInfo {
    @JsonCreator
    public Stream(
        @JsonProperty("name") String name,
        @JsonProperty("topic") String topic,
        @JsonProperty("format") String format
    ) {
      super(name, topic, format);
    }

    public Stream(KsqlStream ksqlStream) {
      this(
          ksqlStream.getName(),
          ksqlStream.getKsqlTopic().getKafkaTopicName(),
          ksqlStream.getKsqlTopic().getKsqlTopicSerDe().getSerDe().name()
      );
    }
  }

  public static class Table extends SourceInfo {
    private final boolean isWindowed;

    @JsonCreator
    public Table(
        @JsonProperty("name") String name,
        @JsonProperty("topic") String topic,
        @JsonProperty("format") String format,
        @JsonProperty("isWindowed") Boolean isWindowed
    ) {
      super(name, topic, format);
      this.isWindowed = isWindowed;
    }

    public Table(KsqlTable ksqlTable) {
      this(
          ksqlTable.getName(),
          ksqlTable.getKsqlTopic().getKafkaTopicName(),
          ksqlTable.getKsqlTopic().getKsqlTopicSerDe().getSerDe().name(),
          ksqlTable.isWindowed()
      );
    }

    public boolean getIsWindowed() {
      return isWindowed;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      Table table = (Table) o;
      return isWindowed == table.isWindowed;
    }

    @Override
    public int hashCode() {
      return Objects.hash(super.hashCode(), isWindowed);
    }
  }

  public String getName() {
    return name;
  }

  public String getTopic() {
    return topic;
  }

  public String getFormat() {
    return format;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SourceInfo that = (SourceInfo) o;
    return Objects.equals(name, that.name)
           && Objects.equals(topic, that.topic)
           && Objects.equals(format, that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, topic, format);
  }
}
