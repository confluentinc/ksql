package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import java.util.Objects;

public class LagListResponse {
  private final Map<String, Map<Integer, Map<String, Long>>> allCurrentPositions;

  @JsonCreator
  public LagListResponse(
      @JsonProperty("allCurrentPositions") final Map<String, Map<Integer, Map<String, Long>>>
          allCurrentPositions
  ) {
    this.allCurrentPositions = allCurrentPositions;
  }

  public Map<String, Map<Integer, Map<String, Long>>> getAllCurrentPositions() {
    return allCurrentPositions;
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
    return Objects.equals(allCurrentPositions, that.allCurrentPositions);
  }

  @Override
  public int hashCode() {
    return Objects.hash(allCurrentPositions);
  }
}
