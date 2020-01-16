package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class LagInfoEntity {

  private final long currentOffsetPosition;
  private final long endOffsetPosition;
  private final long offsetLag;

  @JsonCreator
  public LagInfoEntity(
      @JsonProperty("currentOffsetPosition") final long currentOffsetPosition,
      @JsonProperty("endOffsetPosition") final long endOffsetPosition,
      @JsonProperty("offsetLag") final long offsetLag
  ) {
    this.currentOffsetPosition = currentOffsetPosition;
    this.endOffsetPosition = endOffsetPosition;
    this.offsetLag = offsetLag;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LagInfoEntity that = (LagInfoEntity) o;
    return currentOffsetPosition == that.currentOffsetPosition
        && endOffsetPosition == that.endOffsetPosition && offsetLag == that.offsetLag;
  }

  @Override
  public int hashCode() {
    return Objects.hash(currentOffsetPosition, endOffsetPosition, offsetLag);
  }

  @Override
  public String toString() {
    return currentOffsetPosition + "," + endOffsetPosition + "," + offsetLag;
  }

  public long getCurrentOffsetPosition() {
    return currentOffsetPosition;
  }

  public long getEndOffsetPosition() {
    return endOffsetPosition;
  }

  public long getOffsetLag() {
    return offsetLag;
  }
}
