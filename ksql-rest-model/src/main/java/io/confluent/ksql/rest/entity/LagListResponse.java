package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

public class LagListResponse {
  private final Map<String, Map<Integer, Map<String, LagInfoEntity>>> allLags;

  @JsonCreator
  public LagListResponse(
      @JsonProperty("allLags") final Map<String, Map<Integer, Map<String, LagInfoEntity>>> allLags
  ) {
    this.allLags = allLags;
  }

  public Map<String, Map<Integer, Map<String, LagInfoEntity>>> getAllLags() {
    return allLags;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LagListResponse that = (LagListResponse) o;
    return Objects.equals(allLags, that.allLags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allLags);
  }
}
