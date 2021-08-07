package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.List;
import java.util.Optional;

public class QueryResponseRow {

  public final List<?> row;
  public final Optional<String> token;

  public QueryResponseRow(
      final @JsonProperty(value = "row") List<?> row,
      final @JsonProperty(value = "token") Optional<String> token
  ) {
    this.row = row;
    this.token = token;
  }
}
