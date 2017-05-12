package io.confluent.ksql.rest.json;

import io.confluent.ksql.metastore.KSQLStream;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class StreamsList extends KSQLStatementResponse {
  private final List<KSQLStream> streams;

  public StreamsList(String statementText, List<KSQLStream> streams) {
    super(statementText);
    this.streams = streams;
  }

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
