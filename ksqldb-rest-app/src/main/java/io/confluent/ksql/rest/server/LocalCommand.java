package io.confluent.ksql.rest.server;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * A command which is executed locally and possibly requiring cleanup next time the server is
 * restarted.
 */
public class LocalCommand {

  private final String queryApplicationId;
  private final Type type;

  @JsonCreator
  public LocalCommand(
      @JsonProperty("type") final Type type,
      @JsonProperty("queryApplicationId") final String queryApplicationId
  ) {
    this.type = type;
    this.queryApplicationId = queryApplicationId;
  }

  public String getQueryApplicationId() {
    return queryApplicationId;
  }

  public Type getType() {
    return type;
  }

  public enum Type {
    TRANSIENT_QUERY
  }
}
