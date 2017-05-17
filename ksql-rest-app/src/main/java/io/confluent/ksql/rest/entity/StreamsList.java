package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.KSQLStream;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@JsonTypeName("streams")
public class StreamsList extends KSQLEntity {
  private final Collection<StreamInfo> streams;

  @JsonCreator
  public StreamsList(
      @JsonProperty("statementText") String statementText,
      @JsonProperty("streams")       Collection<StreamInfo> streams
  ) {
    super(statementText);
    this.streams = streams;
  }

  public static StreamsList fromKsqlStreams(String statementText, Collection<KSQLStream> ksqlStreams) {
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
    private final Schema schema;
    private final Field keyField;
    private final TopicsList.TopicInfo topicInfo;

    @JsonCreator
    public StreamInfo(
        @JsonProperty("name")      String name,
        @JsonProperty("schema")    Schema schema,
        @JsonProperty("keyField")  Field keyField,
        @JsonProperty("topicInfo") TopicsList.TopicInfo topicInfo
    ) {
      this.name = name;
      this.schema = schema;
      this.keyField = keyField;
      this.topicInfo = topicInfo;
    }

    public StreamInfo(KSQLStream ksqlStream) {
      this(
          ksqlStream.getName(),
          ksqlStream.getSchema(),
          ksqlStream.getKeyField(),
          new TopicsList.TopicInfo(ksqlStream.getKsqlTopic())
      );
    }

    public String getName() {
      return name;
    }

    public Schema getSchema() {
      return schema;
    }

    public Field getKeyField() {
      return keyField;
    }

    public TopicsList.TopicInfo getTopicInfo() {
      return topicInfo;
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
          Objects.equals(getSchema(), that.getSchema()) &&
          Objects.equals(getKeyField(), that.getKeyField()) &&
          Objects.equals(getTopicInfo(), that.getTopicInfo());
    }

    @Override
    public int hashCode() {
      return Objects.hash(getName(), getSchema(), getKeyField(), getTopicInfo());
    }
  }
}
