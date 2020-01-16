package io.confluent.ksql.rest.entity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class LagReportingResponse {

  private final boolean isOk;

  @JsonCreator
  public LagReportingResponse(
      @JsonProperty("ok") final boolean ok
  ) {
    this.isOk = ok;
  }

  public boolean getIsOk() {
    return isOk;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }

    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final LagReportingResponse that = (LagReportingResponse) o;
    return isOk == that.isOk;
  }

  @Override
  public int hashCode() {
    return Objects.hash(isOk);
  }
}
