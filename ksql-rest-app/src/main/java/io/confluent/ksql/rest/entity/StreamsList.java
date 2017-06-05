/**
 * Copyright 2017 Confluent Inc.
 **/
package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
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

  public static StreamsList fromKsqlStreams(String statementText, Collection<KsqlStream> ksqlStreams) {
    Collection<StreamInfo> streamInfos = ksqlStreams.stream().map(StreamInfo::new).collect(Collectors.toList());
    return new StreamsList(statementText, streamInfos);
  }

  @JsonUnwrapped
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

    @JsonCreator
    public StreamInfo(
        @JsonProperty("name")  String name,
        @JsonProperty("topic") String topic
    ) {
      this.name = name;
      this.topic = topic;
    }

    public StreamInfo(KsqlStream ksqlStream) {
      this(
          ksqlStream.getName(),
          ksqlStream.getKsqlTopic().getName()
      );
    }

    public String getName() {
      return name;
    }

    public String getTopic() {
      return topic;
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
      return Objects.equals(getName(), that.getName()) &&
          Objects.equals(getTopic(), that.getTopic());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getTopic());
    }
  }
}
