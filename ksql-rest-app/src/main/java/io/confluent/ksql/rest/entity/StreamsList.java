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
import com.fasterxml.jackson.annotation.JsonTypeName;
import io.confluent.ksql.metastore.KsqlStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeName("streams")
public class StreamsList extends KsqlEntity {
  private final Collection<StreamInfo> streams;

  @JsonCreator
  public StreamsList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("streams")       Collection<StreamInfo> streams
  ) {
    super(statementText);
    this.streams = streams;
  }

  public static StreamsList fromKsqlStreams(
      String statementText,
      Collection<KsqlStream> ksqlStreams
  ) {
    Collection<StreamInfo> streamInfos =
        ksqlStreams.stream().map(StreamInfo::new).collect(Collectors.toList());
    return new StreamsList(statementText, streamInfos);
  }

  public List<StreamInfo> getStreams() {
    return new ArrayList<>(streams);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof StreamsList)) {
      return false;
    }
    StreamsList that = (StreamsList) o;
    return Objects.equals(getStreams(), that.getStreams());
  }

  @Override
  public int hashCode() {
    return Objects.hash(getStreams());
  }

  public static class StreamInfo {
    private final String name;
    private final String topic;
    private final String format;

    @JsonCreator
    public StreamInfo(
        @JsonProperty("name")  String name,
        @JsonProperty("topic") String topic,
        @JsonProperty("format") String format
    ) {
      this.name = name;
      this.topic = topic;
      this.format = format;
    }

    public StreamInfo(KsqlStream ksqlStream) {
      this(
          ksqlStream.getName(),
          ksqlStream.getKsqlTopic().getKafkaTopicName(),
          ksqlStream.getKsqlTopic().getKsqlTopicSerDe().getSerDe().name()
      );
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
      if (!(o instanceof StreamInfo)) {
        return false;
      }
      StreamInfo that = (StreamInfo) o;
      return Objects.equals(getName(), that.getName())
          && Objects.equals(getTopic(), that.getTopic())
          && Objects.equals(getFormat(), that.getFormat());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getTopic(), getFormat());
    }
  }
}
