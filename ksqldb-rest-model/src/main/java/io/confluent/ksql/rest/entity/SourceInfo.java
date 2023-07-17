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

import static java.util.Objects.requireNonNull;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.confluent.ksql.rest.entity.SourceInfo.Stream;
import io.confluent.ksql.rest.entity.SourceInfo.Table;
import java.util.Objects;
import java.util.Optional;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "STREAM", value = Stream.class),
    @JsonSubTypes.Type(name = "TABLE", value = Table.class),
})
public abstract class SourceInfo {

  private final String name;
  private final String topic;
  private final String keyFormat;
  private final String valueFormat;
  private final boolean windowed;

  SourceInfo(
      final String name,
      final String topic,
      final String keyFormat,
      final String valueFormat,
      final boolean windowed
  ) {
    this.name = requireNonNull(name, "name");
    this.topic = requireNonNull(topic, "topic");
    this.keyFormat = requireNonNull(keyFormat, "keyFormat");
    this.valueFormat = requireNonNull(valueFormat, "valueFormat");
    this.windowed = windowed;
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Stream extends SourceInfo {

    @Deprecated // Since 0.13 "format" is not being sent. Remove in future release.
    @JsonCreator
    public Stream(
        @JsonProperty("name") final String name,
        @JsonProperty("topic") final String topic,
        @JsonProperty("keyFormat") final Optional<String> keyFormat,
        @JsonProperty("valueFormat") final Optional<String> valueFormat,
        @JsonProperty("isWindowed") final Optional<Boolean> windowed,
        @JsonProperty("format") final Optional<String> legacyValueFormat
    ) {
      this(
          name,
          topic,
          keyFormat.orElse("KAFKA"),
          valueFormat.orElse(legacyValueFormat.orElse("UNKNOWN")),
          windowed.orElse(false)
      );
    }

    public Stream(
        final String name,
        final String topic,
        final String keyFormat,
        final String valueFormat,
        final boolean windowed
    ) {
      super(name, topic, keyFormat, valueFormat, windowed);
    }
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Table extends SourceInfo {

    @Deprecated // Since 0.13 "format" is not being sent. Remove in future release.
    @JsonCreator
    public Table(
        @JsonProperty("name") final String name,
        @JsonProperty("topic") final String topic,
        @JsonProperty("keyFormat") final Optional<String> keyFormat,
        @JsonProperty("valueFormat") final Optional<String> valueFormat,
        @JsonProperty("isWindowed") final Optional<Boolean> windowed,
        @JsonProperty("format") final Optional<String> legacyValueFormat
    ) {
      this(
          name,
          topic,
          keyFormat.orElse("KAFKA"),
          valueFormat.orElse(legacyValueFormat.orElse("UNKNOWN")),
          windowed.orElse(false)
      );
    }

    public Table(
        final String name,
        final String topic,
        final String keyFormat,
        final String valueFormat,
        final boolean windowed
    ) {
      super(name, topic, keyFormat, valueFormat, windowed);
    }
  }

  public String getName() {
    return name;
  }

  public String getTopic() {
    return topic;
  }

  public String getKeyFormat() {
    return keyFormat;
  }

  public String getValueFormat() {
    return valueFormat;
  }

  @JsonProperty("isWindowed")
  public boolean isWindowed() {
    return windowed;
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
    return windowed == that.windowed
        && Objects.equals(name, that.name)
        && Objects.equals(topic, that.topic)
        && Objects.equals(keyFormat, that.keyFormat)
        && Objects.equals(valueFormat, that.valueFormat);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, topic, keyFormat, valueFormat, windowed);
  }
}
