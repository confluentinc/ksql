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
import io.confluent.ksql.metastore.KsqlStream;
import io.confluent.ksql.metastore.KsqlTable;
import io.confluent.ksql.rest.entity.SourceInfo.Stream;
import io.confluent.ksql.rest.entity.SourceInfo.Table;
import java.util.Objects;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "STREAM", value = Stream.class),
    @JsonSubTypes.Type(name = "TABLE", value = Table.class),
    })
public class SourceInfo {
  private final String name;
  private final String topic;
  private final String format;

  public SourceInfo(final String name, final String topic, final String format) {
    this.name = name;
    this.topic = topic;
    this.format = format;
  }

  public static class Stream extends SourceInfo {
    @JsonCreator
    public Stream(
        @JsonProperty("name") final String name,
        @JsonProperty("topic") final String topic,
        @JsonProperty("format") final String format
    ) {
      super(name, topic, format);
    }

    public Stream(final KsqlStream ksqlStream) {
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
        @JsonProperty("name") final String name,
        @JsonProperty("topic") final String topic,
        @JsonProperty("format") final String format,
        @JsonProperty("isWindowed") final Boolean isWindowed
    ) {
      super(name, topic, format);
      this.isWindowed = isWindowed;
    }

    public Table(final KsqlTable ksqlTable) {
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
    public boolean equals(final Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      if (!super.equals(o)) {
        return false;
      }
      final Table table = (Table) o;
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
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final SourceInfo that = (SourceInfo) o;
    return Objects.equals(name, that.name)
           && Objects.equals(topic, that.topic)
           && Objects.equals(format, that.format);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, topic, format);
  }
}
