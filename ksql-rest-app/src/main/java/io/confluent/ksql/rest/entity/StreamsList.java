package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import io.confluent.ksql.metastore.KSQLStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@JsonTypeName("streams")
public class StreamsList extends KSQLEntity {
  private final List<KSQLStream> streams;

  public StreamsList(String statementText, List<KSQLStream> streams) {
    super(statementText);
    this.streams = streams;
  }

  @JsonUnwrapped
  public List<KSQLStream> getStreams() {
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
}
